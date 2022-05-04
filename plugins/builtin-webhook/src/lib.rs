extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;
use flowrunner::utils::*;

extern crate json_ops;
use json_ops::JsonOps;

use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;

//use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use tokio::runtime::Runtime;
use async_trait::async_trait;

use log::{debug, info, error};
//use tracing::*;

use axum::Router;
use axum::routing::*;
use axum::extract::{Path, Json, Extension};
use axum::response::IntoResponse;
use axum::http::StatusCode;

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct Webhook {
    host_addr: String,
    hooks: Vec<Hook>,
}

//impl Clone for Webhook {
    //fn clone(&self) -> Self {
        //Webhook {
            //ds: None,
            //ops: self.ops.clone()
        //}
    //}
//}


#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct Hook {
    name: String,
    format: Option<HashMap<String, String>>,
}

#[async_trait]
impl Plugin for Webhook {
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
        let mut default = Webhook::default();

        match jops_params.get_value_e::<String>("host_addr") {
            Ok(v) => default.host_addr = v,
            Err(e) => { return Err(anyhow!(e)); },
        };

        match jops_params.get_value_e::<Vec<Hook>>("hooks") {
            Ok(v) => default.hooks = v,
            Err(e) => { return Err(anyhow!(e)); },
        };

        *self = default;

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}


    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, sender: Option<String>, rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let shared_sender = Arc::new(sender.unwrap_or_else(|| self.host_addr.clone()));
        let shared_hooks = Arc::new(self.hooks.clone());

        // Initialize tracing
        //tracing_subscriber::fmt::init();

        // Build our application with a route
        let app = Router::new()
            .route("/webhook/:hook", post(handler))
            .layer(Extension(rx.clone()))
            .layer(Extension(shared_sender))
            .layer(Extension(shared_hooks));

        // run our app with hyper
        // `axum::Server` is a re-export of `hyper::Server`
        let addr = match self.host_addr.parse::<SocketAddrV4>() {
            Ok(a) => a,
            Err(e) => { error!("{}", e); return_plugin_exec_result_err!(result, e.to_string()); },
        };

        info!("Listening on {}", addr);
        if let Err(e) = axum::Server::bind(&SocketAddr::V4(addr))
            .serve(app.into_make_service())
            .await {
            return_plugin_exec_result_err!(result, e.to_string());
        }

        result.status = Status::Ok;
        result
    }
}

async fn handler(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Path(hook): Path<String>,
    Json(payload): Json<Value>,
    Extension(rx): Extension<Vec<Sender<FlowMessage>>>,
    Extension(sender): Extension<Arc<String>>,
    Extension(hooks): Extension<Arc<Vec<Hook>>>,
) -> impl IntoResponse {

    if !hooks.iter().any(|h| h.name == hook) {
        return StatusCode::NOT_FOUND
    }

    debug!("webhook payload received: {:?}", payload);
    // Check hook
    let msg = FlowMessage::JsonWithSender {
        uuid: generate_uuid(),
        sender: sender.to_string(),
        source: Some(hook),
        value: payload,
    };

    for rx1 in rx.iter() {
        match rx1.send(msg.clone()).await {
            Ok(()) => (),
            Err(e) => error!("{}", e.to_string()),
        };
    }

    StatusCode::OK
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin Webhook loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(Webhook::default()))
}

#[cfg(test)]
mod tests {
    use reqwest::header::CONTENT_TYPE;

    use super::*;
    use tokio::time::{sleep, Duration};
    use async_channel::bounded;
    use serde_json::json;

    #[tokio::test]
    async fn test_func() {
        let (rx, tx) = bounded::<FlowMessage>(1024);
        let rxs = vec![rx.clone()];
        let txs = vec![tx.clone()];

        let mut webhook = Webhook::default();

        let params: Map<String, Value> = serde_json::from_str(r#"{
            "host_addr": "127.0.0.1:3000",
            "hooks": [
                {
                    "name": "hook1"
                }
            ]
        }"#).unwrap();

        webhook.validate_params(params).unwrap();

        tokio::spawn(async move{
            let _ = webhook.func(Some("webhook".to_string()), &rxs, &vec![]).await;
        });

        // Send a post request
        let client = reqwest::Client::new();
        let mut response = client.post("http://localhost:3000/webhook/hook2")
            .header(CONTENT_TYPE, "application/json")
            .body(r#"{"message": "hello world"}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(StatusCode::NOT_FOUND, response.status());

        response = client.post("http://localhost:3000/webhook/hook1")
            .header(CONTENT_TYPE, "application/json")
            .body(r#"{"message": "hello world"}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, response.status());

        sleep(Duration::from_millis(1000)).await;

        let result = txs[0].recv().await.unwrap();

        let expected = FlowMessage::JsonWithSender{
            sender: "webhook".to_string(),
            source: Some("hook1".to_string()),
            value: json!({"message": "hello world"})
        };

        assert_eq!(expected, result);
    }
}
