use serde_json::{Value, Number, Map};

use tokio::time::{sleep, Duration};

use flowrunner::plugin::PluginRegistry;
use flowrunner::flow::Flow;
//use flowrunner::test::utils::*;

//use json_ops::json_map;
use reqwest::header::CONTENT_TYPE;

#[tokio::test]
async fn httpserver_get() {
    //init_log();
    let _ = env_logger::try_init();

    let content = r#"
name: flow1

kind: stream
sources:
- name: webserver
  plugin: builtin-httpserver
  params:
    host_addr: "127.0.0.1:3000"
    routes:
      - path: /routes/:route
        method: GET
        result:
          job: job-1
          task: task-1
          payload: stdout
        result_job: job-1
        result_task: task-1
      - path: /routes
        method: POST
        result:
          job: job-2
          task: task-1
          payload: stdout.payload

jobs:
- hosts: localhost
  if: "{{ context.sender == 'webserver' and context.source == '/routes/:route' }}"
  tasks:
  - builtin-shell:
      params:
        cmd: "echo '{\"message\": \"unknown\"}'"

- hosts: localhost
  if: "{{ context.sender == 'webserver' and context.source == '/routes' }}"
  tasks:
  - builtin-shell:
      params:
        cmd: "echo {{ context.data| json_encode() | safe }}"
"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    tokio::spawn(async move {
        let _ = flow.run().await;
    });

    let client = reqwest::Client::new();
    let resp1 = client.get("http://localhost:3000/routes/route1")
        .header(CONTENT_TYPE, "application/json")
        //.body(r#"{"message": "hello world"}"#)
        .send()
        .await
        .unwrap();

    let resp2 = client.post("http://localhost:3000/routes")
        .header(CONTENT_TYPE, "application/json")
        .body(r#"{"message": "hello world"}"#)
        .send()
        .await
        .unwrap();

    println!("resp1: {:#?}", resp1.text().await.unwrap());
    println!("resp2: {:#?}", resp2.text().await.unwrap());

    sleep(Duration::from_millis(10000)).await;
}
