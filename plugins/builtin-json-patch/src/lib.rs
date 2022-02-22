extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;

extern crate json_ops;
use json_ops::JsonOps;

//use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::value::Value;
use serde_json::Map;

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use async_trait::async_trait;

use log::debug;

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct JsonPatch {
    target: Value,
    patch: Vec<Op>,
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct Op {
    path: String,
    action: String,
    value: Option<Value>,
}

#[async_trait]
impl Plugin for JsonPatch {
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

        // Check Target
        match jops_params.get_value_e::<Value>("target") {
            Ok(v) => self.target = v,
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        // Check Patch
        match jops_params.get_value_e::<Vec<Op>>("patch") {
            Ok(v) => {
                if v.len() <= 0 {
                    return Err(anyhow!("Patch must not be empty"));
                }

                // Check each operation
                for o in v.iter() {
                    // Check if path or action is empty
                    if o.path == "" || o.action == "" {
                        return Err(anyhow!("Path or Op must not be empty"));
                    }

                    // Check if op is supported
                    let ops = vec!["add", "replace", "remove"];

                    if !ops.contains(&o.action.as_str()) {
                        return Err(anyhow!("Op must have one of the following values: add, replace & remove"));
                    }

                    // Check if value is None when action is add or replace
                    if (o.action.as_str() == "add" || o.action.as_str() == "replace") && o.value.is_none() {
                        return Err(anyhow!("Value must be specified when Op is add or replace"));
                    }
                }

                self.patch = v;
            },
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}

    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, _sender: Option<String>, _rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let mut json_ops = JsonOps::new(self.target.clone());

        for (idx, op) in self.patch.iter().enumerate() {
            if op.action == "replace" {
                if let Some(v) = op.value.clone() {
                    if let Err(e) = json_ops.set_value_by_path(op.path.as_str(), v) {
                        return_plugin_exec_result_err!(result, format!("op[{}], path {}: {}", idx, op.path, e.to_string()));
                    }
                } else {
                    return_plugin_exec_result_err!(result, format!("op[{}], path {}: for replace action, value must be specified", idx, op.path));
                }
            }

            if op.action == "add" {
                if let Some(v) = op.value.clone() {
                    if let Err(e) = json_ops.add_value_by_path(op.path.as_str(), v) {
                        return_plugin_exec_result_err!(result, format!("op[{}], path {}: {}", idx, op.path, e.to_string()));
                    }
                } else {
                    return_plugin_exec_result_err!(result, format!("op[{}], path {}: for add action, value must be specified", idx, op.path));
                }
            }

            if op.action == "remove" {
                if let Err(e) = json_ops.remove_value_by_path(op.path.as_str()) {
                    return_plugin_exec_result_err!(result, format!("op[{}], path {}: {}", idx, op.path, e.to_string()));
                }
            }
        }

        result.output.insert("result".to_string(), json_ops.get_json_value(""));
        result.status = Status::Ok;
        result
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin JsonPatch loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(JsonPatch::default()))
}

#[cfg(test)]
mod tests {
    use flowrunner::plugin_exec_result;

    use super::*;

    #[tokio::test]
    async fn test_replace() {
        let txs = Vec::<Sender<FlowMessage>>::new();
        let rxs = Vec::<Receiver<FlowMessage>>::new();

        let mut jsonpatch = JsonPatch::default();

        let params: Map<String, Value> = serde_json::from_str(r#"{
            "target": {
                "string": "text",
                "boolean": true,
                "int": 5,
                "float": 12.5,
                "arr_str": ["str1", "str2", "str3"],
                "arr_int": [1, 2, 3],
                "arr_bool": [true, false, true],
                "arr_mixted": [1, true, "str1", 12.5],
                "object": {
                    "field1": "text1",
                    "field2": 3,
                    "field3": [1, 2, 3]
                }
            },
            "patch": [
                {
                    "path": "string",
                    "action": "replace",
                    "value": "new_text"
                },
                {
                    "path": "boolean",
                    "action": "replace",
                    "value": false
                },
                {
                    "path": "int",
                    "action": "replace",
                    "value": 10
                },
                {
                    "path": "float",
                    "action": "replace",
                    "value": 15.6
                },
                {
                    "path": "arr_str.1",
                    "action": "replace",
                    "value": "newstr2"
                },
                {
                    "path": "arr_int.2",
                    "action": "replace",
                    "value": 3000
                },
                {
                    "path": "arr_bool.1",
                    "action": "remove"
                },
                {
                    "path": "arr_mixted.0",
                    "action": "replace",
                    "value": 10
                },
                {
                    "path": "arr_mixted.3",
                    "action": "remove"
                },
                {
                    "path": "arr_mixted",
                    "action": "add",
                    "value": "newstr"
                },
                {
                    "path": "object.field2",
                    "action": "replace",
                    "value": 3000
                },
                {
                    "path": "object.field3.2",
                    "action": "replace",
                    "value": 1
                },
                {
                    "path": "object.field4",
                    "action": "add",
                    "value": {
                        "field41": 1,
                        "field42": true
                    }
                }
            ]
        }"#).unwrap();

        jsonpatch.validate_params(params).unwrap();

        let expected = plugin_exec_result!(
            Status::Ok,
            "",
            "result" => serde_json::from_str(r#"{
                "string": "new_text",
                "boolean": false,
                "int": 10,
                "float": 15.6,
                "arr_str": ["str1", "newstr2", "str3"],
                "arr_int": [1, 2, 3000],
                "arr_bool": [true, true],
                "arr_mixted": [10, true, "str1", "newstr"],
                "object": {
                    "field1": "text1",
                    "field2": 3000,
                    "field3": [1, 2, 1],
                    "field4": {
                        "field41": 1,
                        "field42": true
                    }
                }
        }"#).unwrap());

        let result = jsonpatch.func(None, &txs, &rxs).await;

        assert_eq!(expected, result);
    }
}
