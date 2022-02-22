#[macro_use]
extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::datastore::store::BoxStore;

extern crate json_ops;
use json_ops::JsonOps;

use serde::{Deserialize, Serialize};
//use std::collections::HashMap;
use serde_json::Value;
use serde_json::{Map, Number};

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use tokio::runtime::Runtime;
use async_trait::async_trait;

use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{Row, FromRow, Column, TypeInfo};

use chrono::{DateTime, Utc, NaiveDate, NaiveDateTime, NaiveTime};
use std::str::FromStr;

use evalexpr::*;
use regex::Regex;

use log::debug;

// Our plugin implementation
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
struct Pgql {
    conn_str: String,
    max_conn: u32,
    stmts: Vec<Stmt>,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
struct Stmt {
    stmt: String,
    cond: String,
    params: Vec<StmtParam>,
    fetch: String
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
struct StmtParam {
    value: Value,
    pg_type: String,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
struct PgqlRow {
    columns: Map<String, Value>,
}

/// Implement conversion from PgRow to PgqlRow
///
/// In this function, we try convert Postgres types in SQLX to serde_json::Value. That is why, some
/// types can not be converted. Other complexe types need to be supported in the futur.
impl<'r> FromRow<'r, PgRow> for PgqlRow {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let cols = row.columns();
        debug!("Row's columns: {cols:#?}");

        let mut columns: Map<String, Value> = Map::new();

        for i in 0..cols.len() {
            let name = cols[i].name();
            let type_info = cols[i].type_info().name().to_lowercase();

            match type_info.as_str() {
                "bool" => {
                    let val: bool = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::Bool(val));
                },
                "char" => {
                    let val: i8 = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::Number(Number::from(val)));
                },
                "int" | "serial" | "int4" => {
                    let val: i32 = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::Number(Number::from(val)));
                },
                "smallint" | "smallserial" | "int2" => {
                    let val: i16 = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::Number(Number::from(val)));
                },
                "bigint" | "bigserial" | "int8" => {
                    let val: i64 = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::Number(Number::from(val)));
                },
                "double precision" | "float8" | "real" | "float4" => {
                    let val: f64 = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::Number(Number::from_f64(val).unwrap_or(Number::from(0))));
                },
                "varchar" | "char(n)" | "text" | "name" => {
                    let val: String = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::String(val));
                },
                "timestamptz" => {
                    let val: DateTime<Utc> = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::String(val.to_string()));
                },
                "timestamp" => {
                    let val: NaiveDateTime = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::String(val.to_string()));
                },
                "date" => {
                    let val: NaiveDate = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::String(val.to_string()));
                },
                "time" => {
                    let val: NaiveTime = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::String(val.to_string()));
                },
                "uuid" => {
                    let val: uuid::Uuid = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::String(val.to_string()));
                },
                //"inet" | "cidr" => {
                    //let val: ipnetwork::IpNetwork = row.try_get(i)?;

                    //columns.insert(name.to_string(), Value::String(val.to_string()));
                //},
                "macaddr" => {
                    let val: mac_address::MacAddress = row.try_get(i)?;

                    columns.insert(name.to_string(), Value::String(val.to_string()));
                },
                "json" | "jsonb" => {
                    let val: Value = row.try_get(i)?;

                    columns.insert(name.to_string(), val);
                },
                _ => {
                    return Err(sqlx::Error::TypeNotFound{type_name: type_info});
                },
            }
        }

        Ok(PgqlRow { columns })
    }
}

#[macro_export]
/// This macro tries to convert json type to postgres types without really knowing in advance.
///
/// Except for bool, some scalar types, a json string can be any postgres types. So when we get a
/// json string, we are going to try to convert to one by one.
macro_rules! bind_query {
    ($qry:expr, $p:expr, $result:expr) => {
        {

            match &$p.value {
                Value::Bool(b) => $qry = $qry.bind(b),
                Value::String(s) => {
                    match $p.pg_type.to_lowercase().as_str() {
                        "bool" => {
                            let val = match s.parse::<bool>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "char" => {
                            let val = match s.parse::<i8>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "int" | "serial" | "int4" => {
                            let val = match s.parse::<i32>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "smallint" | "smallserial" | "int2" => {
                            let val = match s.parse::<i16>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "bigint" | "bigserial" | "int8" => {
                            let val = match s.parse::<i64>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "double precision" | "float8" | "real" | "float4" => {
                            let val = match s.parse::<f64>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "varchar" | "char(n)" | "text" | "name" => {
                            let val = match s.parse::<String>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "timestamptz" => {
                            let val = match s.parse::<DateTime<Utc>>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "timestamp" => {
                            let val = match s.parse::<NaiveDateTime>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "date" => {
                            let val = match s.parse::<NaiveDate>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "time" => {
                            let val = match s.parse::<NaiveTime>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "uuid" => {
                            let val = match s.parse::<uuid::Uuid>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        //"inet" | "cidr" => {
                            //let val: ipnetwork::IpNetwork = row.try_get(i)?;

                            //columns.insert(name.to_string(), Value::String(val.to_string()));
                        //},
                        "macaddr" => {
                            let val = match s.parse::<mac_address::MacAddress>() {
                                Ok(v) => Some(v),
                                Err(_) => None,
                            };

                            $qry = $qry.bind(val);
                        },
                        "json" | "jsonb" => {
                            $qry = $qry.bind(s);
                        },
                        _ => {
                            $qry = $qry.bind(s);
                        },
                    }
                },
                Value::Number(n) => {
                    if n.is_f64() {
                        $qry = $qry.bind(n.as_f64());
                    } else {
                        $qry = $qry.bind(n.as_i64());
                    }
                },
                Value::Null => {
                    match $p.pg_type.to_lowercase().as_str() {
                        "bool" => {
                            let val: Option<bool> = None;
                            $qry = $qry.bind(val);
                        },
                        "char" => {
                            let val: Option<i8> = None;
                            $qry = $qry.bind(val);
                        },
                        "int" | "serial" | "int4" => {
                            let val: Option<i32> = None;
                            $qry = $qry.bind(val);
                        },
                        "smallint" | "smallserial" | "int2" => {
                            let val: Option<i16> = None;
                            $qry = $qry.bind(val);
                        },
                        "bigint" | "bigserial" | "int8" => {
                            let val: Option<i64> = None;
                            $qry = $qry.bind(val);
                        },
                        "double precision" | "float8" | "real" | "float4" => {
                            let val: Option<f64> = None;
                            $qry = $qry.bind(val);
                        },
                        "varchar" | "char(n)" | "text" | "name" => {
                            let val: Option<String> = None;
                            $qry = $qry.bind(val);
                        },
                        "timestamptz" => {
                            let val: Option<DateTime<Utc>> = None;
                            $qry = $qry.bind(val);
                        },
                        "timestamp" => {
                            let val: Option<NaiveDateTime> = None;
                            $qry = $qry.bind(val);
                        },
                        "date" => {
                            let val: Option<NaiveDate> = None;
                            $qry = $qry.bind(val);
                        },
                        "time" => {
                            let val: Option<NaiveTime> = None;
                            $qry = $qry.bind(val);
                        },
                        "uuid" => {
                            let val: Option<uuid::Uuid> = None;
                            $qry = $qry.bind(val);
                        },
                        //"inet" | "cidr" => {
                            //let val: ipnetwork::IpNetwork = row.try_get(i)?;

                            //columns.insert(name.to_string(), Value::String(val.to_string()));
                        //},
                        "macaddr" => {
                            let val: Option<mac_address::MacAddress> = None;
                            $qry = $qry.bind(val);
                        },
                        "json" | "jsonb" => {
                            let val: Option<String> = None;
                            $qry = $qry.bind(val);
                        },
                        _ => {
                            return_plugin_exec_result_err!($result, format!("Postgres param's type {} not supported for Null value", $p.pg_type));
                        },
                    }
                },
                _ => {
                    return_plugin_exec_result_err!($result, "Postgres param value's type not supported".to_string());
                },
            };
        }
    }
}


#[async_trait]
impl Plugin for Pgql {
    fn get_name(&self) -> String {
        env!("CARGO_PKG_NAME").to_string()
    }

    fn get_version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }

    fn get_description(&self) -> String {
        env!("CARGO_PKG_DESCRIPTION").to_string()
    }

    fn get_params(&self) -> Map<String, Value> {
        let params: Map<String, Value> = Map::new();

        params
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}

    fn validate_params(&mut self, params: Map<String, Value>) -> Result<()> {
        let jops_params = JsonOps::new(Value::Object(params));

        // Check URL
        match jops_params.get_value_e::<String>("conn_str") {
            Ok(s) => self.conn_str = s,
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        // Check Method
        match jops_params.get_value_e::<u32>("max_conn") {
            Ok(u) => self.max_conn = u,
            Err(_) => self.max_conn = 5,
        };

        // Check Headers (optional)
        match jops_params.get_value_e::<Vec<Stmt>>("stmts") {
            Ok(stmts) => self.stmts = stmts,
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        Ok(())
    }

    /// Execute sql statements
    ///
    /// This function will execute sql statements in a given list. Firstly, it does a simple check
    /// with regexp to determine if the statement is an "execute" or "query" and then execute one
    /// by one.
    ///
    /// If a statement starts with INSERT/DELETE/UPDATE/CREATE/DROP, it will be considered as
    /// "execute" and the result being returned is the number of rows affected.
    ///
    /// If a statement starts wtih SELECT or has RETURNING, it will be considered as "query" and
    /// the result being returned will be PgqlRow.
    ///
    /// In the "execute" statement, the parameter "fetch" is ignored.
    ///
    /// If any error occured, the function will stop, rollback all operations and return.
    async fn func(&self, _sender: Option<String>, _rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
       let _ =  env_logger::try_init();

        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let mut result = PluginExecResult::default();

        let pool = match PgPoolOptions::new()
            .max_connections(self.max_conn)
            .connect(&self.conn_str).await {
            Ok(p) => p,
            Err(e) => {
                return_plugin_exec_result_err!(result, e.to_string());
            },
        };

        let mut transaction = match pool.begin().await {
            Ok(tx) => tx,
            Err(e) => { return_plugin_exec_result_err!(result, e.to_string()); },
        };

        for (idx, st) in self.stmts.iter().enumerate() {
            let cond = st.cond.clone();

            if eval_boolean(cond.as_str()).unwrap_or(false) {
                let stmt = st.stmt.as_str();
                // Parse query
                let qry_type = sql_parser(&stmt);

                if qry_type.as_str() == "query" {
                    let mut qry = sqlx::query_as::<_, PgqlRow>(stmt.clone());

                    for p in st.params.iter() {
                        bind_query!(qry, p, result);
                    }

                    debug!("Executing query: {}", stmt);

                    // Fetch data according to fetch type
                    let res = match st.fetch.as_str() {
                        "one" => {
                            match qry.fetch_one(&mut transaction).await {
                                Ok(row) => Ok(Value::Array(vec![Value::Object(row.columns)])),
                                Err(e) => Err(e),
                            }
                        },
                        _ => {
                            match qry.fetch_all(&mut transaction).await {
                                Ok(rows) => {
                                    let mut rs: Vec<Value> = Vec::new();
                                    for r in rows.into_iter() {
                                        rs.push(Value::Object(r.columns));
                                    }

                                    Ok(Value::Array(rs))
                                },
                                Err(e) => Err(e),
                            }
                        },
                    };

                    match res {
                        Ok(rows) => {
                            result.output.insert(idx.to_string(), rows);
                        },
                        Err(e) => {
                            let _ = transaction.rollback().await;

                            return_plugin_exec_result_err!(result, e.to_string());
                        },
                    };
                } else { // statement = execute
                    let mut qry = sqlx::query(stmt.clone());

                    for p in st.params.iter() {
                        bind_query!(qry, p, result);
                    }

                    debug!("Executing statement: {}", stmt);

                    match qry.execute(&mut transaction).await {
                        Ok(res) => {
                            result.output.insert(idx.to_string(), Value::Number(Number::from(res.rows_affected())));
                        },
                        Err(e) => {
                            let _ = transaction.rollback().await.unwrap();

                            return_plugin_exec_result_err!(result, e.to_string());
                        }
                    };
                }
            }
        }

        let _ = transaction.commit().await;

        result.status = Status::Ok;

        result
    }
}

fn sql_parser(stmt: &str) -> String {
    let re = Regex::new(r"(\w+) (.*)").unwrap();

    let caps = match re.captures(stmt) {
        Some(c) => c,
        None => return "".to_string(),
    };

    let cap1 = caps.get(1).map_or("", |m| m.as_str()).to_lowercase();
    let cap2 = caps.get(2).map_or("", |m| m.as_str()).to_lowercase();

    match cap1.as_str() {
        "update" |
        "delete" |
        "insert" => {
            if cap2.contains("returning") {
                return "query".to_string();
            }

            "execute".to_string()
        },
        "drop" |
        "create" => "execute".to_string(),
        "select" => "query".to_string(),
        _ => "".to_string(),
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin Pgql loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(Pgql::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowrunner::plugin_exec_result;
    use json_ops::json_map;
    use tokio::sync::*;

    //use std::time::Duration;
    //use tokio::time::{sleep, Duration};

    async fn init_db() {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://flowrunner:flowrunner@localhost:5432/flowrunner")
            .await
            .unwrap();

        sqlx::query(r#"DROP TABLE IF EXISTS users;"#)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query(r#"
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    username VARCHAR ( 50 ) UNIQUE NOT NULL,
    password character varying(64)NOT NULL,
    enabled boolean,
    age integer,
    created_at timestamp with time zone DEFAULT now()
  );"#)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[test]
    fn test_sql_parser() {
        let mut stmt = "INSERT INTO users(username, password) VALUES ($1, $2);";

        let mut qry_type = sql_parser(stmt);

        assert_eq!("execute".to_string(), qry_type);

        stmt = "INSERT INTO users(username, password) VALUES ($1, $2) returning id;";
        qry_type = sql_parser(stmt);

        assert_eq!("query".to_string(), qry_type);

        stmt = "INSERT INTO distributors (did, dname) VALUES (7, 'Redline GmbH')
    ON CONFLICT (did) DO UPDATE SET dname = EXCLUDED.dname;";
        qry_type = sql_parser(stmt);

        assert_eq!("execute".to_string(), qry_type);

        stmt = "UPDATE users SET password = $1 WHERE username = $2";
        qry_type = sql_parser(stmt);

        assert_eq!("execute".to_string(), qry_type);

        stmt = "UPDATE users SET password = $1 WHERE username = $2 returning username";
        qry_type = sql_parser(stmt);

        assert_eq!("query".to_string(), qry_type);

        stmt = "select * from users;";
        qry_type = sql_parser(stmt);

        assert_eq!("query".to_string(), qry_type);
    }

    //#[tokio::test]
    // Disable this test because we still encounter the following errors:
    // ---- tests::test_func stdout ----
    // thread 'tests::test_func' panicked at 'Cannot drop a runtime in a context where blocking is not allowed. Th s happens when a runtime is dropped from within an asynchronous context.', /Users/thanhnguyen/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.12.0/src/runtime/blocking/shutdown.rs:51:21
    async fn test_func() {
        init_db().await;

        let mut json = r#"{
            "conn_str": "postgres://flowrunner:flowrunner@localhost:5432/flowrunner",
            "max_conn": 5,
            "stmts": [
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age) VALUES ($1, $2, $3, $4);",
                    "cond": "true",
                    "params": [
                        {"value": "test1", "pg_type": "varchar"},
                        {"value": "pass1", "pg_type": "varchar"},
                        {"value": false, "pg_type": "bool"},
                        {"value": 5, "pg_type": "int"}
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users (toto) VALUES ($1);",
                    "cond": "true",
                    "params": [
                        {"value": "test2", "pg_type": "varchar"}
                    ],
                    "fetch": ""
                }
            ]
        }"#;

        let txs = Vec::<Sender<FlowMessage>>::new();
        let rxs = Vec::<Receiver<FlowMessage>>::new();

        let mut value: Value = serde_json::from_str(json).unwrap();
        let mut params = value.as_object().unwrap().to_owned();
        let mut pg = Pgql::default();

        pg.validate_params(params.clone()).unwrap();

        let (tx, rx) = oneshot::channel();
        let pg_cloned = pg.clone();
        let txs_cloned = txs.clone();
        let rxs_cloned = rxs.clone();
        tokio::spawn(async move {
            let res = pg_cloned.func(None, &txs_cloned, &rxs_cloned).await;
            tx.send(res).unwrap();
        });

        let mut expected = plugin_exec_result!(
            Status::Ko,
            "error returned from database: column \"toto\" of relation \"users\" does not exist",
            "0" => Value::Number(Number::from(1))
        );

        let mut result = rx.await.unwrap();

        assert_eq!(expected, result);

        // Check rollback when an error occured
        let pool = PgPoolOptions::new().connect(&pg.conn_str).await.unwrap();
        let res = sqlx::query("SELECT * FROM users;").fetch_all(&pool).await.unwrap();

        assert_eq!(0, res.len());

        // Check sequence of statements
        json = r#"{
            "conn_str": "postgres://flowrunner:flowrunner@localhost:5432/flowrunner",
            "max_conn": 5,
            "stmts": [
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5);",
                    "cond": "true",
                    "params": [
                        {"value": "test1", "pg_type": "varchar"},
                        {"value": "pass1", "pg_type": "varchar"},
                        {"value": false, "pg_type": "bool"},
                        {"value": 5, "pg_type": "int"},
                        {"value": "2022-01-31 01:16:14.043462 UTC", "pg_type": "timestamptz"}
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5);",
                    "cond": "true",
                    "params": [
                        {"value": "test2", "pg_type": "varchar"},
                        {"value": "pass2", "pg_type": "varchar"},
                        {"value": false, "pg_type": "bool"},
                        {"value": 5, "pg_type": "int"},
                        {"value": "2022-01-31 01:16:14.043462 UTC", "pg_type": "timestamptz"}
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (username) DO UPDATE SET enabled = EXCLUDED.enabled, age = EXCLUDED.age;",
                    "cond": "true",
                    "params": [
                        {"value": "test2", "pg_type": "varchar"},
                        {"value": "pass2", "pg_type": "varchar"},
                        {"value": true, "pg_type": "bool"},
                        {"value": 6, "pg_type": "int"},
                        {"value": "2022-01-31 01:16:14.043462 UTC", "pg_type": "timestamptz"}
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5);",
                    "cond": "true",
                    "params": [
                        {"value": "test3", "pg_type": "varchar"},
                        {"value": "pass3", "pg_type": "varchar"},
                        {"value": false, "pg_type": "bool"},
                        {"value": 7, "pg_type": "int"},
                        {"value": "2022-01-31 01:16:14.043462 UTC", "pg_type": "timestamptz"}
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "SELECT * FROM users;",
                    "cond": "true",
                    "params": [],
                    "fetch": "all"
                },
                {
                    "stmt": "UPDATE users SET created_at = $1::TIMESTAMP WHERE id = $2;",
                    "cond": "true",
                    "params": [
                        {"value": "2999-01-30 11:03:53 UTC", "pg_type": "timestamptz"},
                        {"value": 1, "pg_type": "int"}
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "DELETE FROM users WHERE id = $1;",
                    "cond": "true",
                    "params": [
                        {"value": 3, "pg_type": "int"}
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "SELECT * FROM users;",
                    "cond": "true",
                    "params": [],
                    "fetch": "all"
                }
            ]
        }"#;

        value = serde_json::from_str(json).unwrap();
        params = value.as_object().unwrap().to_owned();

        pg.validate_params(params.clone()).unwrap();

        let (tx, rx) = oneshot::channel();
        let pg_cloned = pg.clone();
        let txs_cloned = txs.clone();
        let rxs_cloned = rxs.clone();
        tokio::spawn(async move {
            let res = pg_cloned.func(None, &txs_cloned, &rxs_cloned).await;
            tx.send(res).unwrap();
        });

        //println!("{result:#?}");

        expected = plugin_exec_result!(
            Status::Ok,
            "",
            "0" => Value::Number(Number::from(1)),
            "1" => Value::Number(Number::from(1)),
            "2" => Value::Number(Number::from(1)),
            "3" => Value::Number(Number::from(1)),
            "4" => Value::Array(vec![
                Value::Object(json_map!(
                    "id" => Value::Number(Number::from(2)),
                    "username" => Value::String("test1".to_string()),
                    "password" => Value::String("pass1".to_string()),
                    "enabled" => Value::Bool(false),
                    "age" => Value::Number(Number::from(5)),
                    "created_at" => Value::String("2022-01-31 01:16:14.043462 UTC".to_string())
                )),
                Value::Object(json_map!(
                    "id" => Value::Number(Number::from(3)),
                    "username" => Value::String("test2".to_string()),
                    "password" => Value::String("pass2".to_string()),
                    "enabled" => Value::Bool(true),
                    "age" => Value::Number(Number::from(6)),
                    "created_at" => Value::String("2022-01-31 01:16:14.043462 UTC".to_string())
                )),
                Value::Object(json_map!(
                    "id" => Value::Number(Number::from(5)),
                    "username" => Value::String("test3".to_string()),
                    "password" => Value::String("pass3".to_string()),
                    "enabled" => Value::Bool(false),
                    "age" => Value::Number(Number::from(7)),
                    "created_at" => Value::String("2022-01-31 01:16:14.043462 UTC".to_string())
                ))
            ]),
            "5" => Value::Number(Number::from(0)),
            "6" => Value::Number(Number::from(1)),
            "7" => Value::Array(vec![
                Value::Object(json_map!(
                    "id" => Value::Number(Number::from(2)),
                    "username" => Value::String("test1".to_string()),
                    "password" => Value::String("pass1".to_string()),
                    "enabled" => Value::Bool(false),
                    "age" => Value::Number(Number::from(5)),
                    "created_at" => Value::String("2022-01-31 01:16:14.043462 UTC".to_string())
                )),
                Value::Object(json_map!(
                    "id" => Value::Number(Number::from(5)),
                    "username" => Value::String("test3".to_string()),
                    "password" => Value::String("pass3".to_string()),
                    "enabled" => Value::Bool(false),
                    "age" => Value::Number(Number::from(7)),
                    "created_at" => Value::String("2022-01-31 01:16:14.043462 UTC".to_string())
                ))
            ])
        );

        result = rx.await.unwrap();
        assert_eq!(expected, result);
    }
}
