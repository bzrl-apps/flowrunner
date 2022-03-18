extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::datastore::store::BoxStore;

extern crate json_ops;
use json_ops::JsonOps;

//use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::{Map, Number};

use std::collections::HashMap;

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use async_trait::async_trait;
use tokio::runtime::Runtime;

use log::*;

use reqwest::{Method, StatusCode, Body};
use reqwest::header::{HeaderName, HeaderValue, HeaderMap};

// Our plugin implementation
#[derive(Debug, Default, Clone)]
struct Uri {
    url: String,
    method: Method,
    headers: HeaderMap,
    status_codes: Vec<StatusCode>,
    body: Option<String>,

    include_resp_headers: bool,
    include_resp_url: bool,
    include_resp_remote_addr: bool,
    include_resp_content_length: bool,
    include_resp_cookies: bool,
}

#[async_trait]
impl Plugin for Uri {
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
        let mut default = Uri::default();

        // Check URL
        match jops_params.get_value_e::<String>("url") {
            Ok(u) => default.url = u,
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        // Check Method
        match jops_params.get_value_e::<String>("method") {
            Ok(m) => {
                match Method::from_bytes(m.as_bytes()) {
                    Ok(m) => default.method = m,
                    Err(e) => { return Err(anyhow!(e)); },
                }
            },
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        // Check Headers (optional)
        match jops_params.get_value_e::<HashMap<String, String>>("headers") {
            Ok(headers) => {
                for (k, v) in headers.iter() {
                    let header = match HeaderName::from_bytes(k.as_bytes()) {
                        Ok(h) => h,
                        Err(e) => { return Err(anyhow!(e)); },
                    };

                    let value = match v.parse::<HeaderValue>() {
                        Ok(v) => v,
                        Err(e) => { return Err(anyhow!(e)); },
                    };

                    default.headers.insert(header, value);
                }
            },
            Err(_) => {
                //return Err(anyhow!(e));
                default.headers = HeaderMap::new();
            },
        };

        // Check StatusCode (optional)
        if let Ok(s) = jops_params.get_value_e::<Vec<u16>>("status_codes") {
            for st in s.iter() {
                match StatusCode::from_u16(*st) {
                    Ok(s) => default.status_codes.push(s),
                    Err(e) => { return Err(anyhow!(e)); },
                }
            }
        } else {
            default.status_codes = vec![StatusCode::OK];
        }

        // Check Body (optional)
        if let Ok(b) = jops_params.get_value_e::<String>("body") {
            default.body = Some(b);
        }

        // Check include_resp_cookies (optional)
        if let Ok(v) = jops_params.get_value_e::<bool>("include_resp_cookies") {
            default.include_resp_cookies = v;
        }

        // Check include_resp_content_length (optional)
        if let Ok(v) = jops_params.get_value_e::<bool>("include_resp_content_length") {
            default.include_resp_content_length = v;
        }

        // Check include_resp_remote_addr (optional)
        if let Ok(v) = jops_params.get_value_e::<bool>("include_resp_remote_addr") {
            default.include_resp_remote_addr = v;
        }

        // Check include_resp_url (optional)
        if let Ok(v) = jops_params.get_value_e::<bool>("include_resp_url") {
            default.include_resp_url = v;
        }

        // Check include_resp_headers (optional)
        if let Ok(v) = jops_params.get_value_e::<bool>("include_resp_headers") {
            default.include_resp_headers = v;
        }

        *self = default;

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}

    async fn func(&self, _sender: Option<String>, _rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let client = reqwest::Client::new();

        // Set URL & method
        let mut req_builder = client.request(self.method.clone(), self.url.clone());

        // Set headers
        if !self.headers.is_empty() {
            req_builder = req_builder.headers(self.headers.clone());
        }

        // Set body
        if let Some(b) = self.body.clone() {
            req_builder = req_builder.body(Body::from(b));
        }

        // Execute request
        match req_builder.send().await {
            Ok(r) => {
                let status = r.status();

                if self.status_codes.is_empty() || self.status_codes.contains(&status) {
                    result.status = Status::Ok;
                } else {
                    result.status = Status::Ko;
                }

                // Prepare output
                result.output.insert("status_code".to_string(), Value::Number(Number::from(status.as_u16())));

                // Handler response's header
                if self.include_resp_headers {
                    let mut headers: Map<String, Value> = Map::new();
                    for (k, v) in r.headers().iter() {
                        headers.insert(k.as_str().to_string(), Value::String(v.to_str().unwrap_or("N/A").to_string()));
                    }

                    result.output.insert("headers".to_string(), Value::Object(headers));
                }

                // Handle content-length
                if self.include_resp_content_length {
                    result.output.insert("content_length".to_string(), Value::Number(Number::from(r.content_length().unwrap_or(0))));
                }

                // Handle url
                if self.include_resp_url {
                    result.output.insert("url".to_string(), Value::String(r.url().as_str().to_string()));
                }

                // Handle SocketAddr
                if self.include_resp_remote_addr {
                    let remote_addr = match r.remote_addr() {
                        Some(ip) => ip.to_string(),
                        None => "N/A".to_string(),
                    };

                    result.output.insert("remote_addr".to_string(), Value::String(remote_addr));
                }

                // Handle cookies
                if self.include_resp_cookies {
                    let mut cookies: Map<String, Value> = Map::new();
                    for cookie in r.cookies() {
                        cookies.insert(cookie.name().to_string(), serde_json::from_str(cookie.value()).unwrap_or(Value::Null));
                    }

                    result.output.insert("cookies".to_string(), Value::Object(cookies));
                }

                // Handle body text
                match r.text().await {
                    Ok(t) => { result.output.insert("content".to_string(), serde_json::from_str(t.as_str()).unwrap_or(Value::Null)); },
                    Err(e) => {
                        result.status = Status::Ko;
                        result.error = e.to_string();

                        return result;
                    },
                }

                return result;

            },
            Err(e) => {
                result.status = Status::Ko;
                result.error = e.to_string();

                return result;
            },
        }
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin Uri loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(Uri::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_func() {
        let json = r#"{
            "url": "https://httpbin.org/post",
            "method": "POST",
            "headers": {
                "accept": "application/json"
            },
            "body": "{\"message\": \"hello world\"}"
        }"#;

        let value: Value = serde_json::from_str(json).unwrap();

        let params = value.as_object().unwrap().to_owned();

        let mut uri = Uri::default();

        uri.validate_params(params.clone()).unwrap();

        tokio::spawn(async move{
            let _ = uri.func(None, &vec![], &vec![]).await;
        });
    }
}
