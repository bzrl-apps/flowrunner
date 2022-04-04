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
use serde_json::{Value, Map, json};

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use tokio::runtime::Runtime;
use async_trait::async_trait;

use log::{debug, info, error};
use tracing::*;

use axum::Router;
use axum::routing::*;
use axum::extract::{Path, Json, Extension, OriginalUri, MatchedPath};
use axum::response::IntoResponse;
use axum::http::StatusCode;

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct HttpServer {
    host_addr: String,
    routes: Vec<Route>,
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct Route {
    path: String,
    method: String,
    status: u16,
    result: String
}

#[async_trait]
impl Plugin for HttpServer {
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
        let mut default = HttpServer::default();

        match jops_params.get_value_e::<String>("host_addr") {
            Ok(v) => default.host_addr = v,
            Err(e) => { return Err(anyhow!(e)); },
        };

        match jops_params.get_value_e::<Vec<Route>>("routes") {
            Ok(v) => default.routes = v,
            Err(e) => { return Err(anyhow!(e)); },
        };

        *self = default;

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}


    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, sender: Option<String>, rx: &Vec<Sender<FlowMessage>>, tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let shared_sender = Arc::new(sender.unwrap_or_else(|| self.host_addr.clone()));
        let shared_routes = Arc::new(self.routes.clone());
        //let shared_tx = Arc::new(Mutex::new(tx.clone()));


        // Initialize tracing
        //tracing_subscriber::fmt::init();

        let mut app = Router::new();
        for r in self.routes.iter() {
            match r.method.as_str() {
                "GET" => app = app.route(r.path.as_str(), get(get_handler)),
                //"POST" => app = app.route(r.path.as_str(), post(post_handler)),
                //"DELETE" => app = app.route(r.path.as_str(), delete(delete_handler)),
                //"PUT" => app = app.route(r.path.as_str(), put(put_handler)),
                _ => return_plugin_exec_result_err!(result, "HTTP Method not supported!".to_string()),
            };

        }
        app = app.layer(Extension(shared_routes))
            .layer(Extension(shared_sender))
            .layer(Extension(rx.clone()))
            .layer(Extension(tx.clone()));

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

async fn get_handler(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    matched_path: MatchedPath,
    OriginalUri(uri): OriginalUri,
    Extension(rx): Extension<Vec<Sender<FlowMessage>>>,
    Extension(tx): Extension<Vec<Receiver<FlowMessage>>>,
    Extension(sender): Extension<Arc<String>>,
    Extension(routes): Extension<Arc<Vec<Route>>>,
) -> impl IntoResponse {

    let path = matched_path.as_str();
    debug!("GET Path: {}", path);

    let route = routes.iter().find(|r| r.path == path && r.method == "GET");
    if let Some(r) = route.clone() {
        let msg = FlowMessage::JsonWithSender {
            sender: sender.to_string(),
            source: Some(r.path.clone()),
            value: json!({
                "route": {
                    "matched_path": matched_path.as_str(),
                    "original_uri": uri.to_string(),
                    "method": r.method,
                    "result": r.result
                },
                "payload": {}
            }),
        };

        for rx1 in rx.iter() {
            if let Err(e) = rx1.send(msg.clone()).await {
                return (StatusCode::BAD_REQUEST, Json(json!({
                    "error": e.to_string()
                })));
            }
        }

        match tx[0].recv().await {
            // Add message received as data in job context
            Ok(msg) => {
                let mut context = Map::new();

                match msg {
                    FlowMessage::JsonWithSender{ sender: s, source: src, value: v } => {
                        context.insert("sender".to_string(), Value::String(s));
                        context.insert("source".to_string(), Value::String(src.unwrap_or_else(|| "".to_string())));
                        context.insert("data".to_string(), v);
                    },
                    _ => {
                        return (StatusCode::BAD_REQUEST, Json(json!({
                            "error": "Message received is not Message::JsonWithSender type"
                        })));
                    },
                }

                let mut text = r.result.clone();
                if render_template(&mut text, &context) {
                    let v_text: Value = serde_json::from_str(&text).unwrap_or(json!({
                        "error": "Cannot serialize rendered result string"
                    }));

                    return (StatusCode::from_u16(r.status).unwrap_or(StatusCode::OK), Json(v_text));
                }

                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                    "error": "Cannot render result template"
                })));
            },
            Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({
                "error": e.to_string()
            }))),
        };
    }

    (StatusCode::NOT_FOUND, Json(Value::Null))
    //// Check route

    //StatusCode::OK
}


fn render_template(text: &mut String, context: &Map<String, Value>) -> bool {
    let mut data: Map<String, Value> = Map::new();

    data.insert("context".to_string(), Value::from(context.clone()));

    expand_env_map(&mut data);

    // Expand job if condition
    if let Err(e) = render_text_template("httpserver", text, &data) {
        error!("httpserver: error to render template {}", e);
        return false
    }

    true
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin HttpServer loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(HttpServer::default()))
}

#[cfg(test)]
mod tests {
    use reqwest::header::CONTENT_TYPE;

    use super::*;
    use tokio::time::{sleep, Duration};
    use async_channel::bounded;
    use serde_json::json;

    #[tokio::test]
    async fn httpserver_func() {
        let (rx, tx) = bounded::<FlowMessage>(1024);
        let rxs = vec![rx.clone()];
        let txs = vec![tx.clone()];

        let mut httpserver = HttpServer::default();

        let params: Map<String, Value> = serde_json::from_str(r#"{
            "host_addr": "127.0.0.1:3000",
            "routes": [
                {
                    "path": "route1",
                    "method": "GET"
                }
            ]
        }"#).unwrap();

        httpserver.validate_params(params).unwrap();

        tokio::spawn(async move{
            let _ = httpserver.func(Some("httpserver".to_string()), &rxs, &vec![]).await;
        });

        // Send a get request
        let client = reqwest::Client::new();
        let mut response = client.get("http://localhost:3000/webroute/route1")
            .header(CONTENT_TYPE, "application/json")
            //.body(r#"{"message": "hello world"}"#)
            .send()
            .await
            .unwrap();

        //assert_eq!(StatusCode::NOT_FOUND, response.status());

        //response = client.post("http://localhost:3000/webroute/route1")
            //.header(CONTENT_TYPE, "application/json")
            //.body(r#"{"message": "hello world"}"#)
            //.send()
            //.await
            //.unwrap();

        //assert_eq!(StatusCode::OK, response.status());

        //sleep(Duration::from_millis(1000)).await;

        //let result = txs[0].recv().await.unwrap();

        //let expected = FlowMessage::JsonWithSender{
            //sender: "httpserver".to_string(),
            //source: Some("route1".to_string()),
            //value: json!({"message": "hello world"})
        //};

        //assert_eq!(expected, result);
    }
}
