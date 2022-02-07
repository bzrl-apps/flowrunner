use serde_json::{Value, Number, Map};

use sqlx::Row;
//use sqlx::types::chrono::DateTime;
use tokio::time::{sleep, Duration};
use rdkafka::producer::future_producer::FutureRecord;

use flowrunner::plugin::{PluginRegistry, PluginExecResult, Status};
use flowrunner::flow::Flow;
use flowrunner::plugin_exec_result;

use json_ops::json_map;

use chrono::{DateTime, Utc};

use crate::utils::*;
mod utils;

#[tokio::test]
async fn test_sink_postgres() {
    //init_log();
    env_logger::init();
    init_kafka(&["topic1"]).await;
    let pool = init_db().await;

    let content = r#"
name: flow1

kind: stream
sources:
- name: kafka1
  plugin: builtin-kafka-consumer
  params:
    brokers:
    - localhost:9092
    consumer:
      group_id: group1
      topics:
      - topic1
      offset: earliest
      options:
        enable.partition.eof: false
        session.timeout.ms: 6000
        enable.auto.commit: true
        auto.commit.interval.ms: 1000
        enable.auto.offset.store: false

jobs:
- hosts: localhost
  tasks:
  - builtin-shell:
      params:
        cmd: "echo {{ context.data | json_encode() | safe }}"

sinks:
- name: pg1
  plugin: builtin-pgql
  params:
    conn_str: "postgres://flowrunner:flowrunner@localhost:5432/flowrunner"
    max_conn: "5"
    stmts:
    - stmt: "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5);"
      cond: "{{ context['data']['task-1']['output']['stdout']['username'] == 'test2' }}"
      params:
        - value: "{{ context['data']['task-1']['output']['stdout']['username'] }}"
          pg_type: varchar
        - value: "{{ context['data']['task-1']['output']['stdout']['password'] }}"
          pg_type: varchar
        - value: "{{ context['data']['task-1']['output']['stdout']['enabled'] }}"
          pg_type: bool
        - value: "{{ context['data']['task-1']['output']['stdout']['age'] }}"
          pg_type: int
        - value: "{{ context['data']['task-1']['output']['stdout']['created_at'] }}"
          pg_type: timestamptz
      fetch: ""
"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    let producer = create_producer_client("localhost:9092");

    let msgs = vec![
        r#"{"username": "test1", "password": "pass1", "enabled": true, "age": 5, "created_at": "2022-01-31 01:16:14.043462 UTC"}"#,
        r#"{"username": "test2", "password": "pass2", "enabled": null, "age": 6, "created_at": "2022-01-31 01:16:14.043462 UTC"}"#
    ];

    let key_bytes = r#"key1"#.as_bytes();

    for m in msgs.iter() {
        let produce_future = producer.send(
            FutureRecord::to("topic1")
                .payload(m.as_bytes())
                .key(key_bytes),
                //.headers(OwnedHeaders::new().add("header_key", "header_value")),
            Duration::from_secs(0),
        );

        match produce_future.await {
            Ok(delivery) => println!("Sent delivery status: {:?}", delivery),
            Err((e, _)) => println!("Sent eror: {:?}", e),
        }
    }

    tokio::spawn(async move {
        flow.run().await;
    });

    sleep(Duration::from_millis(10000)).await;

    // Query directly to database to check results
    let rows = sqlx::query(r#"SELECT * FROM users;"#)
        .fetch_all(&pool)
        .await
        .unwrap();

    let mut result: Vec<Value> = vec![];

    for r in rows.iter() {
        let mut res: Map<String, Value> = Map::new();
        res.insert("id".to_string(), Value::Number(Number::from(r.try_get::<i32, &str>("id").unwrap())));
        res.insert("username".to_string(), Value::String(r.try_get::<String, &str>("username").unwrap()));
        res.insert("password".to_string(), Value::String(r.try_get::<String, &str>("password").unwrap()));

        match r.try_get::<bool, &str>("enabled") {
            Ok(b) => { res.insert("enabled".to_string(), Value::Bool(b)) },
            Err(_) => { res.insert("enabled".to_string(), Value::Null) },
        };

        res.insert("age".to_string(), Value::Number(Number::from(r.try_get::<i32, &str>("age").unwrap())));
        res.insert("created_at".to_string(), Value::String(r.try_get::<DateTime<Utc>, &str>("created_at").unwrap().to_string()));

        result.push(Value::Object(res));
    }

    let expected = Value::Array(vec![
            Value::Object(json_map!(
                "id" => Value::Number(Number::from(1)),
                "username" => Value::String("test2".to_string()),
                "password" => Value::String("pass2".to_string()),
                "enabled" => Value::Null,
                "age" => Value::Number(Number::from(6)),
                "created_at" => Value::String("2022-01-31 01:16:14.043462 UTC".to_string())
            ))
        ]);

    assert_eq!(expected, Value::Array(result));
}

#[tokio::test]
async fn test_job_postgres() {
    //init_log();
    env_logger::init();
    init_kafka(&["topic1"]).await;
    let pool = init_db().await;

    let content = r#"
name: flow1

kind: stream
sources:
- name: kafka1
  plugin: builtin-kafka-consumer
  params:
    brokers:
    - localhost:9092
    consumer:
      group_id: group1
      topics:
      - topic1
      offset: earliest
      options:
        enable.partition.eof: false
        session.timeout.ms: 6000
        enable.auto.commit: true
        auto.commit.interval.ms: 1000
        enable.auto.offset.store: false

jobs:
- hosts: localhost
  tasks:
  - name: pg1
    builtin-pgql:
      params:
        conn_str: "postgres://flowrunner:flowrunner@localhost:5432/flowrunner"
        max_conn: "5"
        stmts:
        - stmt: "INSERT INTO users(username, password, enabled, age, created_at) VALUES ($1, $2, $3, $4, $5);"
          cond: "{{ context['data']['username'] == 'test2' }}"
          params:
          - value: "{{ context['data']['username'] }}"
            pg_type: varchar
          - value: "{{ context['data']['password'] }}"
            pg_type: varchar
          - value: "{{ context['data']['enabled'] }}"
            pg_type: bool
          - value: "{{ context['data']['age'] }}"
            pg_type: int
          - value: "{{ context['data']['created_at'] }}"
            pg_type: timestamptz
          fetch: ""
"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    let producer = create_producer_client("localhost:9092");

    let msgs = vec![
        r#"{"username": "test1", "password": "pass1", "enabled": true, "age": 5, "created_at": "2022-01-31 01:16:14.043462 UTC"}"#,
        r#"{"username": "test2", "password": "pass2", "enabled": null, "age": 6, "created_at": "2022-01-31 01:16:14.043462 UTC"}"#
    ];

    let key_bytes = r#"key1"#.as_bytes();

    for m in msgs.iter() {
        let produce_future = producer.send(
            FutureRecord::to("topic1")
                .payload(m.as_bytes())
                .key(key_bytes),
                //.headers(OwnedHeaders::new().add("header_key", "header_value")),
            Duration::from_secs(0),
        );

        match produce_future.await {
            Ok(delivery) => println!("Sent delivery status: {:?}", delivery),
            Err((e, _)) => println!("Sent eror: {:?}", e),
        }
    }

    tokio::spawn(async move {
        flow.run().await;
    });

    sleep(Duration::from_millis(10000)).await;

    // Query directly to database to check results
    let rows = sqlx::query(r#"SELECT * FROM users;"#)
        .fetch_all(&pool)
        .await
        .unwrap();

    let mut result: Vec<Value> = vec![];

    for r in rows.iter() {
        let mut res: Map<String, Value> = Map::new();
        res.insert("id".to_string(), Value::Number(Number::from(r.try_get::<i32, &str>("id").unwrap())));
        res.insert("username".to_string(), Value::String(r.try_get::<String, &str>("username").unwrap()));
        res.insert("password".to_string(), Value::String(r.try_get::<String, &str>("password").unwrap()));

        match r.try_get::<bool, &str>("enabled") {
            Ok(b) => { res.insert("enabled".to_string(), Value::Bool(b)) },
            Err(_) => { res.insert("enabled".to_string(), Value::Null) },
        };

        res.insert("age".to_string(), Value::Number(Number::from(r.try_get::<i32, &str>("age").unwrap())));
        res.insert("created_at".to_string(), Value::String(r.try_get::<DateTime<Utc>, &str>("created_at").unwrap().to_string()));

        result.push(Value::Object(res));
    }

    let expected = Value::Array(vec![
            Value::Object(json_map!(
                "id" => Value::Number(Number::from(1)),
                "username" => Value::String("test2".to_string()),
                "password" => Value::String("pass2".to_string()),
                "enabled" => Value::Null,
                "age" => Value::Number(Number::from(6)),
                "created_at" => Value::String("2022-01-31 01:16:14.043462 UTC".to_string())
            ))
        ]);

    assert_eq!(expected, Value::Array(result));
}
