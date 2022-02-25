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
    cond: Option<String>,
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
                    let ops = vec!["get", "set", "delete", "find"];

                    if !ops.contains(&o.action.as_str()) {
                        return Err(anyhow!("Op must have one of the following values: get, set, delete & find"));
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
            let cond = op.cond.clone();
            if !eval_boolean(cond.unwrap_or("true".to_string()).as_str()).unwrap_or(false) {
                continue;
            }

            match op.action.as_str() {
                "set" => {
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
                },
                "get" => {
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
                },
                "delete" => {
                    if let Err(e) = store.delete(op.namespace.as_str(), op.key.as_str()) {
                        return_plugin_exec_result_err!(result, format!("op[{}], ns {}, key {}: {}", idx, op.namespace, op.key, e.to_string()));
                    }

                    result.output.insert(idx.to_string(), Value::Object(json_map!(
                            "namespace" => Value::String(op.namespace.clone()),
                            "key" => Value::String(op.key.clone()),
                            "action" => Value::String(op.action.clone())
                    )));
                },
                "find" => {
                    match store.find(op.namespace.as_str(), op.key.as_str()) {
                        Ok(m) => {
                            result.output.insert(idx.to_string(), Value::Object(json_map!(
                                    "namespace" => Value::String(op.namespace.clone()),
                                    "key" => Value::String(op.key.clone()),
                                    "value" => Value::Object(m),
                                    "action" => Value::String(op.action.clone())
                            )));
                        },
                        Err(e) => { return_plugin_exec_result_err!(result, format!("op[{}], ns {}, key {}: {}", idx, op.namespace, op.key, e.to_string())); },
                    }
                },
                _ => { return_plugin_exec_result_err!(result, format!("op[{}], ns {}, key {}: {} unknown", idx, op.namespace, op.key, op.action)) }
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
    use flowrunner::{
        plugin_exec_result,
        datastore::store::StoreConfig,
    };

    use super::*;

    #[tokio::test]
    async fn test_func() {
        let txs = Vec::<Sender<FlowMessage>>::new();
        let rxs = Vec::<Receiver<FlowMessage>>::new();

        let config: StoreConfig = serde_json::from_str(r#"{
            "conn_str": "/tmp/rocksdb",
            "kind": "rocksdb",
            "options": {
                "create_if_missing": true,
                "create_missing_column_families": true
            },
            "ttl": 0,
            "namespaces": [
                {
                    "name": "ns1",
                    "prefix_len": 3,
                    "options": {
                        "create_if_missing": true,
                        "create_missing_column_families": true
                    }
                }
            ]
        }"#).unwrap();

        let db = config.new_store().unwrap();

        let mut datastore = DataStore::default();
        datastore.ds = Some(db);

        let params: Map<String, Value> = serde_json::from_str(r#"{
            "ops": [
                {
                    "namespace": "ns1",
                    "action": "set",
                    "key": "key1",
                    "value": "value1"
                },
                {
                    "namespace": "ns1",
                    "action": "set",
                    "key": "key2",
                    "value": "value2"
                },
                {
                    "namespace": "ns1",
                    "action": "get",
                    "key": "key1"
                },
                {
                    "namespace": "ns1",
                    "action": "set",
                    "key": "key1",
                    "value": "value111"
                },
                {
                    "namespace": "ns1",
                    "action": "find",
                    "key": "key"
                },
                {
                    "namespace": "ns1",
                    "action": "delete",
                    "key": "key2"
                },
                {
                    "namespace": "ns1",
                    "action": "find",
                    "key": "key"
                }
            ]
        }"#).unwrap();

        datastore.validate_params(params).unwrap();

        let expected = plugin_exec_result!(
            Status::Ok,
            "",
            "0" => serde_json::from_str(r#"{
                "namespace": "ns1",
                "key": "key1",
                "value": "\"value1\"",
                "action": "set"
            }"#).unwrap(),
            "1" => serde_json::from_str(r#"{
                "namespace": "ns1",
                "key": "key2",
                "value": "\"value2\"",
                "action": "set"
            }"#).unwrap(),
            "2" => serde_json::from_str(r#"{
                "namespace": "ns1",
                "key": "key1",
                "value": "\"value1\"",
                "action": "get"
            }"#).unwrap(),
            "3" => serde_json::from_str(r#"{
                "namespace": "ns1",
                "key": "key1",
                "value": "\"value111\"",
                "action": "set"
            }"#).unwrap(),
            "4" => serde_json::from_str(r#"{
                "namespace": "ns1",
                "key": "key",
                "value": {
                    "key1": "value111",
                    "key2": "value2"
                },
                "action": "find"
            }"#).unwrap(),
            "5" => serde_json::from_str(r#"{
                "namespace": "ns1",
                "key": "key2",
                "action": "delete"
            }"#).unwrap(),
            "6" => serde_json::from_str(r#"{
                "namespace": "ns1",
                "key": "key",
                "value": {
                    "key1": "value111"
                },
                "action": "find"
            }"#).unwrap()
        );

        let result = datastore.func(None, &txs, &rxs).await;

        assert_eq!(expected, result);
    }
}
