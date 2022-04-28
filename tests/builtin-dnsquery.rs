use serde_json::{Value, Number, Map};

use tokio::time::{sleep, Duration};

use flowrunner::plugin::PluginRegistry;
use flowrunner::flow::Flow;
use flowrunner::test::utils::*;

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
        - name: uthng.me
          rtype: A
          rdata:
            ip: 192.64.119.14
        - name: uthng.me
          rtype: CNAME
          rdata:
            name: 192.64.119.149
        - name: uthng.me
          rtype: A
          rdata:
            name: 192.64.119.149
        - name: content.tessi.eu
          rtype: CNAME
          rdata:
            name: proxy-ssl.webflow.co

"#;

    let mut flow =  Flow::new_from_str(content).unwrap();

    PluginRegistry::load_plugins("target/debug").await;

    tokio::spawn(async move {
        let _ = flow.run().await;
    });

    sleep(Duration::from_millis(10000)).await;

}
