extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;

//use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::{Map, Number};

use tokio::sync::*;
use async_trait::async_trait;

use std::process::Command;

// Our plugin implementation
struct Shell;

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

    async fn func(&self, params: Map<String, Value>, _rx: &Vec<mpsc::Sender<FlowMessage>>, _tx: &Vec<mpsc::Receiver<FlowMessage>>) -> PluginExecResult {
        //let mut result: Map<String, Value> = Map::new();
        let mut result = PluginExecResult::default();

        // Handle params
        let args: Vec<&str> = match params.get(&"cmd".to_string()) {
            None => {
                result.status = Status::Ko;
                result.error = "param `cmd` is not found".to_string();

                return result;
            },
            Some(c) => match c.as_str() {
                Some(c1) => c1.split(' ').collect(),
                None => {
                    result.status = Status::Ko;
                    result.error = "cmd cannot be read".to_string();

                    return result;
                },
            }
        };

        let mut cmd = Command::new(args[0]);
        for i in 1..args.len() {
            cmd.arg(args[i]);
        }

        let output = cmd.output();

        match output {
            Ok(o) => {
                if o.status.success() {
                    result.status = Status::Ok;
                    result.output.insert("rc".to_string(), Value::Number(Number::from(0)));
                    result.output.insert("stdout".to_string(), Value::String(String::from_utf8(o.stdout).unwrap_or("".to_string())));
                } else {
                    result.status = Status::Ko;
                    result.error = String::from_utf8(o.stderr.clone()).unwrap_or("".to_string());
                    result.output.insert("rc".to_string(), Value::Number(Number::from(o.status.code().unwrap_or(-1))));
                    result.output.insert("stderr".to_string(), Value::String(String::from_utf8(o.stderr).unwrap_or("".to_string())));
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
    Box::into_raw(Box::new(Shell {}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    //#[test]
    async fn test_func() {
        let shell = Shell{};

        let mut params: Map<String, Value> = Map::new();
        let mut expected = PluginExecResult::default();

        // Case OK
        params.insert("cmd".to_string(), Value::String("echo Hello world".to_string()));

        expected.status = Status::Ok;
        expected.output.insert("rc".to_string(), Value::Number(Number::from(0)));
        expected.output.insert("stdout".to_string(), Value::String("Hello world\n".to_string()));

        let mut result = shell.func(params.clone()).await;
        assert_eq!(expected, result);

        expected.output.clear();

        // Case Error of execution
        params.insert("cmd".to_string(), Value::String("ls -z".to_string()));

        expected.status = Status::Ko;
        expected.error= "ls: illegal option -- z\nusage: ls [-@ABCFGHLOPRSTUWabcdefghiklmnopqrstuwx1%] [file ...]\n".to_string();
        expected.output.insert("rc".to_string(), Value::Number(Number::from(1)));
        expected.output.insert("stderr".to_string(), Value::String("ls: illegal option -- z\nusage: ls [-@ABCFGHLOPRSTUWabcdefghiklmnopqrstuwx1%] [file ...]\n".to_string()));

        result = shell.func(params.clone()).await;
        assert_eq!(expected, result);

        expected.output.clear();

        // Case fatal error such as command not found
        params.insert("cmd".to_string(), Value::String("hello".to_string()));

        expected.status = Status::Ko;
        expected.error = "No such file or directory (os error 2)".to_string();

        result = shell.func(params.clone()).await;
        assert_eq!(expected, result);
    }
}
