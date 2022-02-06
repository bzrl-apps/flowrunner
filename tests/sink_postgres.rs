use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;

//use tokio::sync::mpsc::*;
//use futures::lock::Mutex;
use async_channel::*;

use anyhow::{Result, anyhow};

use tokio::time::{sleep, Duration};
use serde_json::value::Number;
use std::collections::HashMap;
use rdkafka::{
    config::ClientConfig,
    //message::{Headers, Message},
    producer::future_producer::{FutureProducer, FutureRecord},
};

use flowrunner::plugin::{PluginRegistry, Status as PluginStatus};
use flowrunner::message::Message as FlowMessage;
use flowrunner::flow::Flow;

use crate::utils::*;
mod utils;

#[tokio::test]
async fn test_sink_postgres() {
    //init_log();
    env_logger::init();
    init_kafka(&["topic1"]).await;
    init_db().await;

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
      cond: "{{ context['data']['task-1']['status'] == 'Ok' }}"
      params:
        - "{{ context['data']['task-1']['output']['stdout']['username'] }}"
        - "pass"
        - null
        - 5
        - "2022-01-31 01:16:14.043462 UTC"
      fetch: ""
"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    let producer = create_producer_client("localhost:9092");

    let msg_bytes = r#"{"username": "test1", "password": "pass1", "enabled": true, "age": 5, "created_at": "2022-01-31 01:16:14.043462 UTC"}"#.as_bytes();
    let key_bytes = r#"key1"#.as_bytes();

    let produce_future = producer.send(
        FutureRecord::to("topic1")
            .payload(msg_bytes)
            .key(key_bytes),
            //.headers(OwnedHeaders::new().add("header_key", "header_value")),
        Duration::from_secs(0),
    );

    match produce_future.await {
        Ok(delivery) => println!("Sent delivery status: {:?}", delivery),
        Err((e, _)) => println!("Sent eror: {:?}", e),
    }


    tokio::spawn(async move {
        flow.run().await.unwrap();
    });

    sleep(Duration::from_millis(10000)).await;
}
