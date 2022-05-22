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

use std::io::{Read, Write};
use std::path::Path;
use std::fs;

use regex::Regex;

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct LineInFile {
    #[serde(default)]
    path: String,
    #[serde(default)]
    actions: Vec<Action>
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct Action {
    #[serde(default)]
    regexp: String,
    #[serde(default)]
    line: Option<String>,
    #[serde(default = "default_state")]
    state: String,
}

fn default_state() -> String {
    "present".to_string()
}

#[async_trait]
impl Plugin for LineInFile {
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
        let mut default = LineInFile::default();

        match jops_params.get_value_e::<String>("path") {
            Ok(v) => {
                if v.is_empty() {
                    return Err(anyhow!("path cannot be empty"));
                }

                default.path = v;
            }
            Err(_) => {
                return Err(anyhow!("path must be specified"));
            },
        };

        match jops_params.get_value_e::<Vec<Action>>("actions") {
            Ok(v) => {
                if v.is_empty() {
                    return Err(anyhow!("actions must not be empty"));
                }

                let states = vec!["absent", "present"];

                for a in v.iter() {
                    if !states.contains(&a.state.as_str()) {
                        return Err(anyhow!("state must be one of the following values: {:?}", states));
                    }

                    if a.regexp.is_empty() {
                        return Err(anyhow!("regexp must not be empty"));
                    }

                    if a.state == "present" && a.line.is_none() {
                        return Err(anyhow!("line must not be empty when state is present"));
                    }
                }

                default.actions = v;

            },
            Err(_) => return Err(anyhow!("actions must be specified")),
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

        let path = Path::new(self.path.as_str());
        if !path.exists() {
            return_plugin_exec_result_err!(result, format!("{} does not exist", self.path));
        }

        // Set options according to override
        let mut f_opts = fs::OpenOptions::new();
        f_opts.read(true);

        info!("Open file to read: file={}", self.path.as_str());
        let content = f_opts.open(self.path.as_str())
            .and_then(|mut v| {
                let mut content = String::new();
                match v.read_to_string(&mut content) {
                    Ok(_) => {
                        debug!("File content read: file={}, content={:?}",
                               self.path, content);
                        Ok(content)
                    },
                    Err(e) => Err(e),
                }
            })
            .unwrap_or_default();

        let mut lines: Vec<&str> = content.split('\n').collect();
        #[allow(unused_assignments)]
        let mut line: &str = "";

        for a in self.actions.iter() {
            info!("Doing action: file={}, action={:?}", self.path, a);

            let re = match Regex::new(a.regexp.as_str()) {
                Ok(v) => v,
                Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
            };

            let indexes: Vec<usize> = lines.iter()
                .enumerate()
                .filter(|(_, v)| re.is_match(v))
                .map(|(i, _)| i)
                .collect();

            debug!("List of line's indexes matching regexp: file={}, idx={:?}",
                   self.path, indexes);

            if indexes.is_empty() {
                if a.state == "present" {
                    info!("Writing line at the end of the file: file={}, line={:?}",
                          self.path, a.line);
                    if let Some(l) = &a.line {
                        line = l;
                        lines.push(line);
                    }
                }
            } else {
                for (i, v) in indexes.iter().enumerate() {
                    if a.state == "absent" {
                        info!("Removing line in the file: file={}, line_no={}",
                              self.path, v-i);
                        lines.remove(v-i);
                    } else {
                        info!("Replace line in the file: file={}, line_no={}, line={:?}",
                              self.path, *v, a.line);
                        if let Some(l) = &a.line {
                            line = l;
                            lines[*v] = line;
                        }
                    }
                }
            }
        }

        debug!("Writing new content after actions executed: content={:#?}", lines);

        f_opts.write(true).truncate(true);
        if let Err(e) = f_opts.open(self.path.as_str())
            .and_then(|mut v| {
                v.write_all(lines.join("\n").as_bytes())
                //v.write_all(b"\n")
            }) {
                return_plugin_exec_result_err!(result, e.to_string());
            }

        result.status = Status::Ok;
        result.output.insert("content".to_string(), Value::String(lines.join("\n")));
        result
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin LineInFile loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(LineInFile::default()))
}

#[cfg(test)]
mod tests {
    use flowrunner::plugin_exec_result;

    use super::*;
    use std::process::Command;

    #[tokio::test]
    async fn lineinfile_func() {
        let txs = Vec::<Sender<FlowMessage>>::new();
        let rxs = Vec::<Receiver<FlowMessage>>::new();

        let mut lif = LineInFile::default();

        let _ = Command::new("cp")
            .arg("Cargo.toml")
            .arg("/tmp/Cargo.toml")
            .output()
            .unwrap();

        let params: Map<String, Value> = serde_json::from_str(r#"{
            "path": "/tmp/Cargo.toml",
            "actions": [
                {
                    "regexp": "^a.*",
                    "line": "line_has_been_replaced",
                    "state": "present"
                },
                {
                    "regexp": "toto",
                    "line": "line_has_been_added",
                    "state": "present"
                },
                {
                    "regexp": "^serde",
                    "state": "absent"
                }
            ]
        }"#).unwrap();

        lif.validate_params(params).unwrap();

        let expected = plugin_exec_result!(
            Status::Ok,
            "",
            "content" => Value::String(r#"[package]
name = "builtin-lineinfile"
version = "0.1.0"
line_has_been_replaced
edition = "2021"

# See more keys and their definitions at https://doc.rust-lang.org/cargo/reference/manifest.html

[dependencies]

flowrunner = { path = "../../" }
json_ops = { path = "../../json-ops" }

# Async/await
tokio = { version = "1", features = ["full"] }
line_has_been_replaced
line_has_been_replaced

# Error
line_has_been_replaced

# Log
log = "0.4"
env_logger = "0.8"

# Serialization/deserialization

# Regexp
regex = "1"

[lib]
crate-type = ["dylib"]

line_has_been_added"#.to_string()));

        let result = lif.func(None, &txs, &rxs).await;

        assert_eq!(expected, result);
    }
}
