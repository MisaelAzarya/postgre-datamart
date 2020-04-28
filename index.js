const pg = require('pg');

function ins(pgClient, table, data){
    return new Promise((resolve, reject) => {
        var qry = "INSERT INTO "+table+" VALUES "+data;
        pgClient.query(qry, function(err, results) {
            if (err) {
            console.error(err);
            return reject(err);
            }
            resolve(results);
        })
    })
}

//syntaxnya username:password@server:port/database_name

// untuk yg localhost biasa
// const pgConString = "postgres://postgres:1234@192.168.0.27:5432/staging_transformation";

// untuk yg localhost docker
const pgConString = "postgres://postgres:mysecretpassword@172.17.0.2:5432/staging_transformation";
var clientpg = new pg.Client(pgConString);

// untuk yg loclahost biasa
// const pgConString2 = "postgres://postgres:1234@192.168.0.27:5432/staging_datamart";

// untuk yg localhost docker
const pgConString2 = "postgres://postgres:mysecretpassword@172.17.0.2:5432/staging_datamart";


var clientpg2 = new pg.Client(pgConString2);

clientpg.connect(function(err){
    if(err){
        throw err;
    }
    else{
        console.log("connect");
        clientpg2.connect();
        var pgTable = "CREATE TABLE IF NOT EXISTS dm_city_top10 ("+
                "customer_name VARCHAR(25)," +
                "city VARCHAR(20)," +
                "state VARCHAR(20)," +
                "total NUMERIC(10,4)," +
                "transaksi INTEGER," +
                "rank INTEGER" +
            ");";
        clientpg2.query(pgTable);
        console.log("Create Table dm_city_top10");

        var pquery = "Select customer_name, city, state, total, transaksi, rank FROM(" +
            "Select customer_id, customer_name, city, state, SUM(total_sales) total, COUNT(customer_id) transaksi, " +
                "rank() OVER ( " +
                    "PARTITION BY city " +
                    "ORDER BY SUM(total_sales) DESC " +
                  ") "+
            "FROM stg_superstore " +
            "GROUP BY customer_id, city, customer_name, state " +
        ")rank_filer WHERE RANK < 10 " +
        "GROUP BY city, customer_name, state, total, transaksi, rank "+
        "ORDER BY city, total DESC ";

        clientpg.query(pquery, function(err, res){
            if(err)throw err;
            else{
                var table = "dm_city_top10";
                if(res.rows.length > 0){
                    var full_data = [];
                    for(var x=0;x<res.rows.length;x++){
                        var data = [];
                        var value = Object.values(res.rows[x]);
                        var keys = Object.keys(res.rows[x]);
                        for(var i=0;i<value.length;i++){
                            if(keys[i]=="customer_name"){
                                value[i] = value[i].replace("'","''");
                            }
                            data.push("'" + value[i] + "'");
                        }
                        full_data.push('(' + data.join(', ') + ')');
                    }
                    // console.log(full_data);
                    ins(clientpg2,table,full_data);
                    console.log("Data Input Success");
                }
            }
        });
    }
})