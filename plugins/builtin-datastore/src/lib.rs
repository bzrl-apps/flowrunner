extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;

extern crate json_ops;
use json_ops::{json_map, JsonOps};

use std::fmt;

//use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use serde_json::Map;

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use async_trait::async_trait;

use evalexpr::*;

use log::debug;

// Our plugin implementation
#[derive(Default, Clone)]
struct DataStore {
    ds: Option<BoxStore>,
    ops: Vec<Op>,
}

impl fmt::Debug for DataStore {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("DataStore")
           .field("ops", &self.ops)
           .finish()
    }
}

//impl Clone for DataStore {
    //fn clone(&self) -> Self {
        //DataStore {
            //ds: None,
            //ops: self.ops.clone()
        //}
    //}
//}

#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct Op {
    namespace: String,
    cond: String,
    key: String,
    value: Option<Value>,
    action: String,
}

#[async_trait]
impl Plugin for DataStore {
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
        let ds_params = JsonOps::new(Value::Object(params));

        // Check Patch
        match ds_params.get_value_e::<Vec<Op>>("ops") {
            Ok(v) => {
                if v.len() <= 0 {
                    return Err(anyhow!("Ops must not be empty"));
                }

                // Check each operation
                for o in v.iter() {
                    // Check if path or action is empty
                    if o.namespace == "" || o.key == "" || o.action == "" {
                        return Err(anyhow!("Namspace, Key or Action must not be empty"));
                    }

                    // Check if op is supported
                    let ops = vec!["get", "set", "delete"];

                    if !ops.contains(&o.action.as_str()) {
                        return Err(anyhow!("Op must have one of the following values: get, set & delete"));
                    }

                    // Check if value is None when action is add or replace
                    if o.action.as_str() == "set" && o.value.is_none() {
                        return Err(anyhow!("Value must be specified when Op is set"));
                    }
                }

                self.ops = v;
            },
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        Ok(())
    }

    fn set_datastore(&mut self, datastore: Option<BoxStore>) {
        self.ds = datastore
    }


    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, _sender: Option<String>, _rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let store = match &self.ds {
            Some(s) => s,
            None => {
                return_plugin_exec_result_err!(result, "The field ds (datastore) is None".to_string());
            },
        };

        for (idx, op) in self.ops.iter().enumerate() {
            // Check cond, if false, zap the operation
            if !eval_boolean(op.cond.as_str()).unwrap_or(false) {
                continue;
            }

            if op.action == "set" {
                if let Some(v) = op.value.clone() {
                    let s = serde_json::to_string(&v).unwrap_or("".to_string());

                    if let Err(e) = store.set(op.namespace.as_str(), op.key.as_str(), s.as_str()) {
                        return_plugin_exec_result_err!(result, format!("op[{}], ns {}, key {}: {}", idx, op.namespace, op.key, e.to_string()));
                    }


                    result.output.insert(idx.to_string(), Value::Object(json_map!(
                            "namespace" => Value::String(op.namespace.clone()),
                            "key" => Value::String(op.key.clone()),
                            "value" => Value::String(s),
                            "action" => Value::String(op.action.clone())
                    )));
                } else {
                    return_plugin_exec_result_err!(result, format!("op[{}], ns {}, key {}: for replace action, value must be specified", idx, op.namespace, op.key));
                }
            }

            if op.action == "get" {
                match store.get(op.namespace.as_str(), op.key.as_str()) {
                    Ok(s) => {
                        result.output.insert(idx.to_string(), Value::Object(json_map!(
                                "namespace" => Value::String(op.namespace.clone()),
                                "key" => Value::String(op.key.clone()),
                                "value" => Value::String(s),
                                "action" => Value::String(op.action.clone())
                        )));
                    },
                    Err(e) => { return_plugin_exec_result_err!(result, format!("op[{}], ns {}, key {}: {}", idx, op.namespace, op.key, e.to_string())); },
                }

            }

            if op.action == "delete" {
                if let Err(e) = store.delete(op.namespace.as_str(), op.key.as_str()) {
                    return_plugin_exec_result_err!(result, format!("op[{}], ns {}, key {}: {}", idx, op.namespace, op.key, e.to_string()));
                }

                result.output.insert(idx.to_string(), Value::Object(json_map!(
                        "namespace" => Value::String(op.namespace.clone()),
                        "key" => Value::String(op.key.clone()),
                        "action" => Value::String(op.action.clone())
                )));
            }
        }

        result.status = Status::Ok;
        result
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin DataStore loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(DataStore::default()))
}

#[cfg(test)]
mod tests {
    use flowrunner::plugin_exec_result;

    use super::*;

    #[tokio::test]
    async fn test_replace() {
        let txs = Vec::<Sender<FlowMessage>>::new();
        let rxs = Vec::<Receiver<FlowMessage>>::new();

        let mut datastore = DataStore::default();

        let params: Map<String, Value> = serde_json::from_str(r#"{
            "ops": [
                {
                    "namespace": "ns1",
                    "action": "set",
                    "key": "key1",
                    "value": "new text"
                },
                {
                    "namespace": "ns1",
                    "action": "get",
                    "key": "key1"
                },
                {
                    "namespace": "ns1",
                    "action": "delete",
                    "key": "key1"
                }
            ]
        }"#).unwrap();

        datastore.validate_params(params).unwrap();

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

        let result = datastore.func(None, &txs, &rxs).await;

        assert_eq!(expected, result);
    }
}
