extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;
use flowrunner::utils::*;

extern crate json_ops;
use json_ops::JsonOps;

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
use axum::extract::{Json, Extension, OriginalUri, MatchedPath};
use axum::response::IntoResponse;
use axum::http::StatusCode;

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct HttpServer {
    host_addr: String,
    is_also_sink: bool,
    routes: Vec<HttpRoute>,
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct HttpRoute {
    path: String,
    method: String,
    // Path to task's result sent from job
    result: HttpResult
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct HttpResult {
    job: String,
    task: String,
    // path from task's output
    payload: Option<String>
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
        let mut params: Map<String, Value> = Map::new();

        params.insert("is_also_sink".to_string(), Value::Bool(self.is_also_sink));

        params
    }

    fn validate_params(&mut self, params: Map<String, Value>) -> Result<()> {
        let jops_params = JsonOps::new(Value::Object(params));
        let mut default = HttpServer::default();

        default.is_also_sink = true;

        match jops_params.get_value_e::<String>("host_addr") {
            Ok(v) => default.host_addr = v,
            Err(e) => { return Err(anyhow!(e)); },
        };

        match jops_params.get_value_e::<Vec<HttpRoute>>("routes") {
            Ok(v) => default.routes = v,
            Err(e) => { return Err(anyhow!(e)); },
        };

        *self = default;

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}


    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, sender: Option<String>, tx: &Vec<Sender<FlowMessage>>, rx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
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
            warn!("r.path {}", r.path);
            match r.method.as_str() {
                "GET" => app = app.route(r.path.as_str(), get(get_handler)),
                "POST" => app = app.route(r.path.as_str(), post(post_handler)),
                "DELETE" => app = app.route(r.path.as_str(), delete(delete_handler)),
                "PUT" => app = app.route(r.path.as_str(), put(put_handler)),
                _ => return_plugin_exec_result_err!(result, "HTTP Method not supported!".to_string()),
            };

        }
        app = app.layer(Extension(shared_routes))
            .layer(Extension(shared_sender))
            .layer(Extension(tx.clone()))
            .layer(Extension(rx.clone()));

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
    Extension(tx): Extension<Vec<Sender<FlowMessage>>>,
    Extension(rx): Extension<Vec<Receiver<FlowMessage>>>,
    Extension(sender): Extension<Arc<String>>,
    Extension(routes): Extension<Arc<Vec<HttpRoute>>>,
) -> impl IntoResponse {

    let path = matched_path.as_str();
    debug!("GET: path={}", path);

    let route = routes.iter().find(|r| r.path == path && r.method == "GET");
    let uuid = generate_uuid();
    if let Some(r) = route.clone() {
        let msg = FlowMessage::JsonWithSender {
            uuid: uuid.clone(),
            sender: sender.to_string(),
            source: Some(r.path.clone()),
            value: json!({
                "route": {
                    "matched_path": matched_path.as_str(),
                    "original_uri": uri.to_string(),
                    "method": r.method,
                },
                "payload": {}
            }),
        };

        let (status, value) = handle_message_exchange(r.to_owned(), uuid, msg, tx, rx).await;

        return (status, Json(value));
    }

    (StatusCode::NOT_FOUND, Json(Value::Null))
}

async fn post_handler(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    matched_path: MatchedPath,
    OriginalUri(uri): OriginalUri,
    Json(payload): Json<Value>,
    Extension(tx): Extension<Vec<Sender<FlowMessage>>>,
    Extension(rx): Extension<Vec<Receiver<FlowMessage>>>,
    Extension(sender): Extension<Arc<String>>,
    Extension(routes): Extension<Arc<Vec<HttpRoute>>>,
) -> impl IntoResponse {

    let path = matched_path.as_str();
    debug!("POST: path={}", path);

    let route = routes.iter().find(|r| r.path == path && r.method == "POST");
    let uuid = generate_uuid();
    if let Some(r) = route.clone() {
        let msg = FlowMessage::JsonWithSender {
            uuid: uuid.clone(),
            sender: sender.to_string(),
            source: Some(r.path.clone()),
            value: json!({
                "route": {
                    "matched_path": matched_path.as_str(),
                    "original_uri": uri.to_string(),
                    "method": r.method,
                },
                "payload": payload
            }),
        };

        let (status, value) = handle_message_exchange(r.to_owned(), uuid, msg, tx, rx).await;

        return (status, Json(value));
    }

    (StatusCode::NOT_FOUND, Json(Value::Null))
}

async fn put_handler(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    matched_path: MatchedPath,
    OriginalUri(uri): OriginalUri,
    Json(payload): Json<Value>,
    Extension(tx): Extension<Vec<Sender<FlowMessage>>>,
    Extension(rx): Extension<Vec<Receiver<FlowMessage>>>,
    Extension(sender): Extension<Arc<String>>,
    Extension(routes): Extension<Arc<Vec<HttpRoute>>>,
) -> impl IntoResponse {

    let path = matched_path.as_str();
    debug!("PUT: path={}", path);

    let route = routes.iter().find(|r| r.path == path && r.method == "PUT");
    let uuid = generate_uuid();
    if let Some(r) = route.clone() {
        let msg = FlowMessage::JsonWithSender {
            uuid: uuid.clone(),
            sender: sender.to_string(),
            source: Some(r.path.clone()),
            value: json!({
                "route": {
                    "matched_path": matched_path.as_str(),
                    "original_uri": uri.to_string(),
                    "method": r.method,
                },
                "payload": payload
            }),
        };

        let (status, value) = handle_message_exchange(r.to_owned(), uuid, msg, tx, rx).await;

        return (status, Json(value));
    }

    (StatusCode::NOT_FOUND, Json(Value::Null))
}

async fn delete_handler(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    matched_path: MatchedPath,
    OriginalUri(uri): OriginalUri,
    Extension(tx): Extension<Vec<Sender<FlowMessage>>>,
    Extension(rx): Extension<Vec<Receiver<FlowMessage>>>,
    Extension(sender): Extension<Arc<String>>,
    Extension(routes): Extension<Arc<Vec<HttpRoute>>>,
) -> impl IntoResponse {

    let path = matched_path.as_str();
    debug!("DELETE: path={}", path);

    let route = routes.iter().find(|r| r.path == path && r.method == "DELETE");
    let uuid = generate_uuid();
    if let Some(r) = route.clone() {
        let msg = FlowMessage::JsonWithSender {
            uuid: uuid.clone(),
            sender: sender.to_string(),
            source: Some(r.path.clone()),
            value: json!({
                "route": {
                    "matched_path": matched_path.as_str(),
                    "original_uri": uri.to_string(),
                    "method": r.method,
                },
                "payload": {}
            }),
        };

        let (status, value) = handle_message_exchange(r.to_owned(), uuid, msg, tx, rx).await;

        return (status, Json(value));
    }

    (StatusCode::NOT_FOUND, Json(Value::Null))
}

async fn handle_message_exchange(
    route: HttpRoute,
    uuid: String,
    msg: FlowMessage,
    tx: Vec<Sender<FlowMessage>>,
    rx: Vec<Receiver<FlowMessage>>
) -> (StatusCode, Value) {
    for tx1 in tx.iter() {
        if let Err(e) = tx1.send(msg.clone()).await {
            return (StatusCode::BAD_REQUEST, json!({
                "error": e.to_string()
            }));
        }
    }

    loop {
        match rx[0].recv().await {
            // Add message received as data in job context
            Ok(msg) => {
                debug!("message received: msg={:?}", msg);

                match msg {
                    FlowMessage::JsonWithSender{ uuid: id, sender: s, source: _src, value: v } => {
                        if uuid == id && route.result.job == s {
                            debug!("Got response from job with the same ID: job={}, uuid={}", route.result.job, id);

                            let (status, value) = build_response(&route.result, &v);

                            return (status, value);
                        } else {
                            debug!("Ignore message: uuid_orig={}, uuid={}", uuid, id);
                        }
                    },
                    _ => {
                        return (StatusCode::BAD_REQUEST, json!({
                            "error": "Message received is not Message::JsonWithSender type"
                        }));
                    },
                }
            },
            Err(e) => {
                return (StatusCode::BAD_REQUEST, json!({
                    "error": e.to_string()
                }));
            }
        }
    }
}

fn build_response(result: &HttpResult, v: &Value) -> (StatusCode, Value) {
    debug!("build response with: task={}, payload={:?}, v={:?}", result.task, result.payload, v);

    let jops_value = JsonOps::new(Value::Object(v.as_object().unwrap_or(&Map::new()).to_owned()));

    match jops_value.get_value_e::<Value>(result.task.as_str()) {
        Ok(v) => {
            let jops_task = JsonOps::new(Value::Object(v.as_object().unwrap_or(&Map::new()).to_owned()));
            if let Ok(status) = jops_task.get_value_e::<String>("status") {
                if status == "Ok" {
                    let payload = result.payload
                        .to_owned()
                        .and_then(|p| Some(format!("output.{}", p)))
                        .unwrap_or("output".to_string());
                    return  (StatusCode::OK, jops_task.get_value_e::<Value>(payload.as_str()).unwrap_or(Value::Null).to_owned());
                }

                return (StatusCode::BAD_REQUEST, v.get("error").unwrap_or(&Value::String("unknown error".to_string())).to_owned());
            }

            (StatusCode::BAD_REQUEST, Value::String("cannot determine task's status".to_string()))
        },
        Err(_) => (StatusCode::BAD_REQUEST, json!({"error": format!("cannot get path result {}", result.task).as_str()})),
    }
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
        let (tx, rx) = bounded::<FlowMessage>(1024);
        let txs = vec![tx.clone()];
        let rxs = vec![rx.clone()];

        let mut httpserver = HttpServer::default();

        let params: Map<String, Value> = serde_json::from_str(r#"{
            "host_addr": "127.0.0.1:3000",
            "routes": [
                {
                    "path": "/routes/:route",
                    "method": "GET",
                    "status": 200
                }
            ]
        }"#).unwrap();

        httpserver.validate_params(params).unwrap();

        tokio::spawn(async move{
            let _ = httpserver.func(Some("httpserver".to_string()), &txs, &rxs).await;
        });

        // Send a get request
        let client = reqwest::Client::new();
        let mut response = client.get("http://localhost:3000/routes/route1")
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
