extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::datastore::store::BoxStore;

extern crate json_ops;
use json_ops::JsonOps;

//use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::{Map, Number};

use anyhow::{anyhow, Result};

use async_trait::async_trait;
use async_channel::{Sender, Receiver};

use std::process::Command;

// Our plugin implementation
#[derive(Debug, Default, Clone)]
struct Shell {
    args: Vec<String>
}

#[async_trait]
impl Plugin for Shell {
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

        // Check Cmd
        match jops_params.get_value_e::<String>("cmd") {
            Ok(c) => {
                self.args = c.split(' ').map(|a| a.to_string()).collect();
            },
            Err(e) => {
                return Err(anyhow!(e));
            },
        };

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}

    async fn func(&self, _sender: Option<String>, _rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        //let mut result: Map<String, Value> = Map::new();
        let mut result = PluginExecResult::default();

        let mut cmd = Command::new(self.args[0].clone());
        for i in 1..self.args.len() {
            cmd.arg(self.args[i].clone());
        }

        let output = cmd.output();

        match output {
            Ok(o) => {
                if o.status.success() {
                    result.status = Status::Ok;
                    result.output.insert("rc".to_string(), Value::Number(Number::from(0)));

                    let str_output = String::from_utf8(o.stdout).unwrap_or("".to_string());
                    result.output.insert("stdout".to_string(), serde_json::from_str(&str_output).unwrap_or(Value::String(str_output)));

                } else {
                    result.status = Status::Ko;
                    result.error = String::from_utf8(o.stderr.clone()).unwrap_or("".to_string());
                    result.output.insert("rc".to_string(), Value::Number(Number::from(o.status.code().unwrap_or(-1))));

                    let str_output = String::from_utf8(o.stderr).unwrap_or("".to_string());
                    result.output.insert("stderr".to_string(), serde_json::from_str(&str_output).unwrap_or(Value::String(str_output)));
                }

                return result;
            },
            Err(e) => {
                result.status = Status::Ko;
                result.error = e.to_string();

                return result;
            }
        }
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut dyn Plugin {
    println!("Plugin Shell loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(Shell::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Number;

    #[tokio::test]
    //#[test]
    async fn test_func() {
        let mut shell = Shell::default();

        let mut params: Map<String, Value> = Map::new();
        let mut expected = PluginExecResult::default();
        let txs_empty = Vec::<Sender<FlowMessage>>::new();
        let rxs_empty = Vec::<Receiver<FlowMessage>>::new();

        // Case OK
        params.insert("cmd".to_string(), Value::String("echo Hello world".to_string()));

        expected.status = Status::Ok;
        expected.output.insert("rc".to_string(), Value::Number(Number::from(0)));
        expected.output.insert("stdout".to_string(), Value::String("Hello world\n".to_string()));

        shell.validate_params(params.clone()).unwrap();
        let mut result = shell.func(None, &txs_empty, &rxs_empty).await;
        assert_eq!(expected, result);

        expected.output.clear();

        // Case Error of execution
        params.insert("cmd".to_string(), Value::String("ls -z".to_string()));

        expected.status = Status::Ko;
        expected.error= "ls: illegal option -- z\nusage: ls [-@ABCFGHLOPRSTUWabcdefghiklmnopqrstuwx1%] [file ...]\n".to_string();
        expected.output.insert("rc".to_string(), Value::Number(Number::from(1)));
        expected.output.insert("stderr".to_string(), Value::String("ls: illegal option -- z\nusage: ls [-@ABCFGHLOPRSTUWabcdefghiklmnopqrstuwx1%] [file ...]\n".to_string()));

        shell.validate_params(params.clone()).unwrap();
        result = shell.func(None, &txs_empty, &rxs_empty).await;
        assert_eq!(expected, result);

        expected.output.clear();

        // Case fatal error such as command not found
        params.insert("cmd".to_string(), Value::String("hello".to_string()));

        expected.status = Status::Ko;
        expected.error = "No such file or directory (os error 2)".to_string();

        shell.validate_params(params.clone()).unwrap();
        result = shell.func(None, &txs_empty, &rxs_empty).await;
        assert_eq!(expected, result);
    }
}
