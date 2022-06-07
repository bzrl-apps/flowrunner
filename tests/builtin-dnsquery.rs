use serde_json::{Value, Number, Map};

use tokio::time::{sleep, Duration};
use tokio::sync::oneshot;

use flowrunner::plugin::{PluginRegistry, PluginExecResult, Status};
use flowrunner::flow::Flow;
//use flowrunner::test::utils::*;
use flowrunner::plugin_exec_result;

use json_ops::json_map;


#[tokio::test]
async fn dnsquery_func() {
    //init_log();
    let _ = env_logger::try_init();

    let content = r#"
name: flow1

jobs:
- tasks:
  - builtin-dnsquery:
      params:
        nameserver: "8.8.8.8:53"
        queries:
        - name: edition.cnn.com
          rtype: A
          rdata:
            ip: 151.101.1.67
        - name: edition.cnn.com
          rtype: CNAME
          rdata:
            name: 151.101.1.67
        - name: edition.cnn.com
          rtype: A
          rdata:
            name: 151.101.1.67
        - name: en.wikipedia.org
          rtype: CNAME
          rdata:
            name: dyna.wikimedia.org

"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let _ = flow.run().await;
        tx.send(flow.jobs[0].result["task-1"].clone()).unwrap();
    });

    let result = rx.await.unwrap();

    let expected = plugin_exec_result!(
        Status::Ok,
        "",
        "result" => Value::Array(vec![
            Value::Object(json_map!(
                "name" => Value::String("edition.cnn.com".to_string()),
                "rtype" => Value::String("A".to_string()),
                "rdata" => Value::Object(json_map!(
                    "ip" => Value::String("151.101.1.67".to_string())
                )),
                "result" => Value::String("name must be specified in rdata for record type A".to_string()),
                "status" => Value::Bool(false)
            )),
            Value::Object(json_map!(
                "name" => Value::String("edition.cnn.com".to_string()),
                "rtype" => Value::String("CNAME".to_string()),
                "rdata" => Value::Object(json_map!(
                    "name" => Value::String("151.101.1.67".to_string())
                )),
                "result" => Value::String("none".to_string()),
                "status" => Value::Bool(false)
            )),
            Value::Object(json_map!(
                "name" => Value::String("edition.cnn.com".to_string()),
                "rtype" => Value::String("A".to_string()),
                "rdata" => Value::Object(json_map!(
                    "name" => Value::String("151.101.1.67".to_string())
                )),
                "result" => Value::String("151.101.1.67".to_string()),
                "status" => Value::Bool(true)
            )),
            Value::Object(json_map!(
                "name" => Value::String("en.wikipedia.org".to_string()),
                "rtype" => Value::String("CNAME".to_string()),
                "rdata" => Value::Object(json_map!(
                    "name" => Value::String("dyna.wikimedia.org".to_string())
                )),
                "result" => Value::String("dyna.wikimedia.org.".to_string()),
                "status" => Value::Bool(true)
            ))
        ])
    );

    assert_eq!(serde_json::to_value(expected).unwrap(), result);

    sleep(Duration::from_millis(10000)).await;

}
