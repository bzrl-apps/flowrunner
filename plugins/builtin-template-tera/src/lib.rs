extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;

extern crate json_ops;
use json_ops::JsonOps;

//use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use async_trait::async_trait;

use log::*;

use std::path::Path;
use std::fs;

use tera::{Tera, Context};

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct TeraTemplate {
    src: String,
    dest: String,
    ctx: String,
    #[serde(default)]
    r#override: bool
}

#[async_trait]
impl Plugin for TeraTemplate {
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
        let mut default = TeraTemplate::default();

        match jops_params.get_value_e::<String>("src") {
            Ok(v) => {
                if v.is_empty() {
                    return Err(anyhow!("src cannot be empty"));
                }

                default.src = v;
            }
            Err(_) => {
                return Err(anyhow!("src must be specified"));
            },
        };

        match jops_params.get_value_e::<String>("dest") {
            Ok(v) => {
                if v.is_empty() {
                    return Err(anyhow!("dest cannot be empty"));
                }

                default.dest = v;
            }
            Err(_) => {
                return Err(anyhow!("dest can not be empty"));
            },
        };

        match jops_params.get_value_e::<String>("ctx") {
            Ok(v) => {
                if v.is_empty() {
                    return Err(anyhow!("ctx cannot be empty"));
                }

                default.ctx = v;
            }
            Err(_) => {
                return Err(anyhow!("ctx can not be empty"));
            },
        };

        match jops_params.get_value_e::<bool>("override") {
            Ok(v) => default.r#override = v,
            Err(_) => default.r#override = true,
        };

        *self = default;

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}

    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, _sender: Option<String>, _tx: &Vec<Sender<FlowMessage>>, _rx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let src = Path::new(self.src.as_str());

        // Create an instance Tera
        let tera = if src.is_dir() {
            let tera = match Tera::new(format!("{}/**/*", self.src).as_str()) {
                Ok(t) => t,
                Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
            };

            let dest = Path::new(self.dest.as_str());
            if dest.exists() {
                if !dest.is_dir() {
                    return_plugin_exec_result_err!(result, "src and dest must be the same kind: directory".to_string());
                }
            } else {
                if let Err(e) = fs::create_dir_all(self.dest.as_str()) {
                    return_plugin_exec_result_err!(result, e.to_string());
                }
            }

            tera
        } else if src.is_file() {
            let mut tera = Tera::default();
            if let Err(e) = tera.add_template_file(self.src.as_str(), Some("default")) {
                return_plugin_exec_result_err!(result, e.to_string());
            }

            let dest = Path::new(self.dest.as_str());
            if dest.exists() && !dest.is_file() {
                return_plugin_exec_result_err!(result, "src and dest must be the same kind: file".to_string());
            }

            tera
        } else {
            return_plugin_exec_result_err!(result, "src must be a directory or a file".to_string());
        };

        // Context
        let context = match serde_json::from_str(self.ctx.as_str())
            .map_err(|e| anyhow!(e))
            .and_then(|v| Context::from_value(v)
                      .map_err(|e| anyhow!(e))) {
                Ok(v) => v,
                Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
            };

        // Set options according to override
        let mut f_opts = fs::OpenOptions::new();
        f_opts.write(true);

        if self.r#override {
            f_opts.create(true).truncate(true);
        } else {
            f_opts.create_new(true);
        }

        // If src = file, just one template (default) to render
        if src.is_file() {
            let f_output = match f_opts.open(self.dest.as_str()) {
                Ok(f) => f,
                Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
            };

            if let Err(e) = tera.render_to("default", &context, f_output) {
                return_plugin_exec_result_err!(result, e.to_string());
            }
        } else { // src == directory
            for tpl in tera.get_template_names() {
                let f_output = match f_opts.open(format!("{}/{}", self.dest, tpl)) {
                    Ok(f) => f,
                    Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
                };

                if let Err(e) = tera.render_to(tpl, &context, f_output) {
                    return_plugin_exec_result_err!(result, e.to_string());
                }
            }
        }

        result.status = Status::Ok;
        result
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin TeraTemplate loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(TeraTemplate::default()))
}

#[cfg(test)]
mod tests {
    use flowrunner::plugin_exec_result;

    use super::*;

    #[tokio::test]
    async fn test_replace() {
        let txs = Vec::<Sender<FlowMessage>>::new();
        let rxs = Vec::<Receiver<FlowMessage>>::new();

        let mut tpl = TeraTemplate::default();

        let params: Map<String, Value> = serde_json::from_str(r#"{
        }"#).unwrap();

        tpl.validate_params(params).unwrap();

        let expected = plugin_exec_result!(
            Status::Ok,
            "",
            "result" => serde_json::from_str(r#"{
        }"#).unwrap());

        let result = tpl.func(None, &txs, &rxs).await;

        assert_eq!(expected, result);
    }
}
