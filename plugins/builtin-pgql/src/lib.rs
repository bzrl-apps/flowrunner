#[macro_use]
extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;

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
    params: Vec<Value>,
    fetch: String
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
    ($qry:expr, $p:expr) => {
        {

            match $p {
                Value::Bool(b) => $qry = $qry.bind(b),
                Value::String(s) => {
                    // Try to conver to DateTime<UTC>
                    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
                        $qry = $qry.bind(dt);
                    } else if let Ok(ndt) = s.parse::<NaiveDateTime>() {
                        $qry = $qry.bind(ndt);
                    } else if let Ok(d) = s.parse::<NaiveDate>() {
                        $qry = $qry.bind(d);
                    } else if let Ok(t) = s.parse::<NaiveTime>() {
                        $qry = $qry.bind(t);
                    } else if let Ok(u) = s.parse::<uuid::Uuid>() {
                        $qry = $qry.bind(u);
                    } else if let Ok(m) = s.parse::<mac_address::MacAddress>() {
                        $qry = $qry.bind(m);
                    } else {
                        $qry = $qry.bind(s);
                    }
                },
                Value::Number(n) => {
                    if n.is_f64() {
                        $qry = $qry.bind(n.as_f64().unwrap());
                    } else {
                        $qry = $qry.bind(n.as_i64().unwrap());
                    }
                },
                _ => $qry = $qry.bind($p.as_str().unwrap()),
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
    async fn func(&self, _rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
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
                        bind_query!(qry, p);
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
                        bind_query!(qry, p);
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

//fn bind_query_value<'q, DB>(qry: Query<'q, DB, <DB as HasArguments<'q>>::Arguments>, val: Value) -> Query<'q, DB, <DB as HasArguments<'q>>::Arguments>
//where
    //DB: Database,
    //bool: Encode<'q, DB> + Type<DB>,
    //f64: Encode<'q, DB> + Type<DB>,
    //i64: Encode<'q, DB> + Type<DB>,
    //str: Encode<'q, DB> + Type<DB>,
//{
    //match val {
        //Value::Bool(b) => qry.bind(b),
        //Value::String(s) => qry.bind(s),
        //Value::Number(n) => {
            //if n.is_f64() {
                //return qry.bind(n.as_f64().unwrap());
            //} else if n.is_i64() {
                //return qry.bind(n.as_i64().unwrap());
            //} else {
                //return qry.bind(n.as_u64().unwrap());
            //}
        //}
        //_ => qry.bind(val.as_str().unwrap()),
    //}
//}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin Pgql loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(Pgql::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowrunner::{plugin_exec_result, json_map};

    use sqlx::PgPool;
    //use std::time::Duration;
    use tokio::time::{sleep, Duration};

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

    #[tokio::test]
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
                        "test1",
                        "pass1",
                        false,
                        5
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users (toto) VALUES ($1);",
                    "cond": "true",
                    "params": [
                        "test2"
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

        let mut result = pg.func(&txs, &rxs).await;

        let mut expected = plugin_exec_result!(
            Status::Ko,
            "error returned from database: column \"toto\" of relation \"users\" does not exist",
            "0" => Value::Number(Number::from(1))
        );

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
                        "test1",
                        "pass1",
                        false,
                        5,
                        "2022-01-31 01:16:14.043462 UTC"
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5);",
                    "cond": "true",
                    "params": [
                        "test2",
                        "pass2",
                        false,
                        5,
                        "2022-01-31 01:16:14.043462 UTC"
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (username) DO UPDATE SET enabled = EXCLUDED.enabled, age = EXCLUDED.age;",
                    "cond": "true",
                    "params": [
                        "test2",
                        "pass2",
                        true,
                        6,
                        "2022-01-31 01:16:14.043462 UTC"
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5);",
                    "cond": "true",
                    "params": [
                        "test3",
                        "pass3",
                        false,
                        7,
                        "2022-01-31 01:16:14.043462 UTC"
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
                        "2999-01-30 11:03:53 UTC",
                        1
                    ],
                    "fetch": ""
                },
                {
                    "stmt": "DELETE FROM users WHERE id = $1;",
                    "cond": "true",
                    "params": [3],
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

        result = pg.func(&txs, &rxs).await;

        println!("{result:#?}");

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

        assert_eq!(expected, result);
    }
}
