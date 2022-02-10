use serde_json::{Value, Number, Map};

use sqlx::Row;
//use sqlx::types::chrono::DateTime;
use tokio::time::{sleep, Duration};

use rdkafka::producer::future_producer::FutureRecord;
use rdkafka::Message;
use rdkafka::consumer::Consumer;

use flowrunner::plugin::{PluginRegistry, PluginExecResult, Status};
use flowrunner::flow::Flow;
use flowrunner::plugin_exec_result;

use std::str;

use json_ops::json_map;

use chrono::{DateTime, Utc};

use crate::utils::*;
mod utils;

#[tokio::test]
async fn test_sink_kafka_producer() {
    //init_log();
    env_logger::init();
    init_kafka(&["topic1", "topic2", "topic3"]).await;

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
- name: producer1
  plugin: builtin-kafka-producer
  params:
    brokers:
    - localhost:9092
    options:
      message.timeout.ms: 5000
    messages:
    - topic: topic2
      message: "{{ context['data']['task-1']['output']['stdout']['username'] }}"
      key: "key2"
    - topic: topic3
      message: "{{ context['data']['task-1']['output']['stdout']['password'] }}"
      key: "key3"
"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    let producer = create_producer_client("localhost:9092");

    let msgs = vec![
        r#"{"username": "test1", "password": "pass1", "enabled": true, "age": 5, "created_at": "2022-01-31 01:16:14.043462 UTC"}"#,
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
        let _ = flow.run().await;
    });

    sleep(Duration::from_millis(10000)).await;

    // Create a new consumer to consume topic2 & topic3
    let consumer_checker = create_consumer_client("localhost:9092", "consumer-checker");
    consumer_checker.subscribe(&["topic2", "topic3"]).unwrap();
    match consumer_checker.iter().next().unwrap() {
        Ok(message) => {
            if message.topic() == "topic2" {
                assert_eq!("key2", str::from_utf8(message.key().unwrap()).unwrap());
                assert_eq!("test1", message.payload_view::<str>().unwrap().unwrap());
            } else if message.topic() == "topic3" {
                assert_eq!("key3", str::from_utf8(message.key().unwrap()).unwrap());
                assert_eq!("pass1", message.payload_view::<str>().unwrap().unwrap());
            } else {
                assert!(false, "topic is not either topic2 or topic 3");
            }
        },
        Err(e) => panic!("Error receiving message: {:?}", e),
    }
}

#[tokio::test]
async fn test_job_kafka_producer() {
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
  - name: producer1
    builtin-kafka-producer:
      params:
        brokers:
        - localhost:9092
        options:
          message.timeout.ms: 5000
        messages:
        - topic: topic2
          message: "{{ context['data']['username'] }}"
          key: "key2"
        - topic: topic3
          message: "{{ context['data']['password'] }}"
          key: "key3"
"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    let producer = create_producer_client("localhost:9092");

    let msgs = vec![
        r#"{"username": "test1", "password": "pass1", "enabled": true, "age": 5, "created_at": "2022-01-31 01:16:14.043462 UTC"}"#,
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
        let _ = flow.run().await;
    });

    sleep(Duration::from_millis(10000)).await;

    // Consumer messages in topic2 & topic3
    let consumer_checker = create_consumer_client("localhost:9092", "consumer-checker");
    consumer_checker.subscribe(&["topic2", "topic3"]).unwrap();
    match consumer_checker.iter().next().unwrap() {
        Ok(message) => {
            if message.topic() == "topic2" {
                assert_eq!("key2", str::from_utf8(message.key().unwrap()).unwrap());
                assert_eq!("test1", message.payload_view::<str>().unwrap().unwrap());
            } else if message.topic() == "topic3" {
                assert_eq!("key3", str::from_utf8(message.key().unwrap()).unwrap());
                assert_eq!("pass1", message.payload_view::<str>().unwrap().unwrap());
            } else {
                assert!(false, "topic is not either topic2 or topic 3");
            }
        },
        Err(e) => panic!("Error receiving message: {:?}", e),
    }
}
