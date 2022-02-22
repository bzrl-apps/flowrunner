use flowrunner::plugin::{PluginRegistry, PluginExecResult, Status};
use flowrunner::flow::Flow;
use flowrunner::plugin_exec_result;

use serde_json::{Value, Number, Map};

use tokio::time::{sleep, Duration};
use rdkafka::producer::future_producer::FutureRecord;

use crate::utils::*;
mod utils;

#[tokio::test]
async fn test_datastore() {
    env_logger::init();
    init_kafka(&["topic1"]).await;
    cleanup_rocksdb("/tmp/rocksdb", &["ns1", "ns2"]);

    let content = r#"
name: flow1

kind: stream

datastore:
    conn_str: /tmp/rocksdb
    kind: rocksdb
    options:
      create_if_missing: true
      create_missing_column_families: true
    ttl: 0
    namespaces:
    - name: ns1
      options:
        create_if_missing: true
        create_missing_column_families: true

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
  - builtin-datastore:
      params:
        ops:
        - namespace: ns1
          cond: "true"
          action: save
          key: "{{ context.data.username }}"
          value: "{{ context.data.password }}"
        - namespace: ns1
          cond: "{{ context.data.username == 'test2' }}"
          action: save
          key: "{{ context.data.username }}"
          value: test222
        - namespace: ns1
          cond: "{{ context.data.username == 'test2' }}"
          action: delete
          key: test1
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
        let _ = flow.run().await;
    });

    sleep(Duration::from_millis(10000)).await;

    let db = open_rocksdb("/tmp/rocksdb", &["ns1"], 0);

    let cf_handle = db.cf_handle("ns1").unwrap();

    let mut val = db.get_cf(cf_handle, "test1".as_bytes()).unwrap().unwrap_or("".as_bytes().to_vec());

    assert_eq!("".to_string(), String::from_utf8(val).unwrap());

    val = db.get_cf(cf_handle, "test2".as_bytes()).unwrap().unwrap();

    assert_eq!("\"test222\"".to_string(), String::from_utf8(val).unwrap());
}
