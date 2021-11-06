extern crate flowrunner;
use flowrunner::plugin::Plugin;

//use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::{Map, Number};

use anyhow::{Result, anyhow};
use log::warn;
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

    async fn func(&self, params: Map<String, Value>) -> Result<Map<String, Value>>{
        let mut result: Map<String, Value> = Map::new();

        // Handle params
        let args: Vec<&str> = match params.get(&"cmd".to_string()) {
            None => return Err(anyhow!("param `cmd` is not found")),
            Some(c) => match c.as_str() {
                Some(c1) => c1.split(' ').collect(),
                None => return Err(anyhow!("cmd cannot be read")),
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
                    result.insert("rc".to_string(), Value::Number(Number::from(0)));
                    result.insert("stdout".to_string(), Value::String(String::from_utf8(o.stdout).unwrap_or("".to_string())));
                } else {
                    result.insert("rc".to_string(), Value::Number(Number::from(o.status.code().unwrap_or(-1))));
                    result.insert("stderr".to_string(), Value::String(String::from_utf8(o.stderr).unwrap_or("".to_string())));
                }

                return Ok(result);
            },
            Err(e) => {
                result.insert("rc".to_string(), Value::Number(Number::from(1)));
                result.insert("stderr".to_string(), Value::String(e.to_string()));

                return Ok(result);
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
        let mut expected: Map<String, Value> = Map::new();

        // Case OK
        params.insert("cmd".to_string(), Value::String("echo Hello world".to_string()));

        expected.insert("rc".to_string(), Value::Number(Number::from(0)));
        expected.insert("stdout".to_string(), Value::String("Hello world\n".to_string()));

        let mut result = shell.func(params.clone()).await.unwrap();
        assert_eq!(expected, result);

        expected.clear();

        // Case Error of execution
        params.insert("cmd".to_string(), Value::String("ls -z".to_string()));

        expected.insert("rc".to_string(), Value::Number(Number::from(1)));
        expected.insert("stderr".to_string(), Value::String("ls: illegal option -- z\nusage: ls [-@ABCFGHLOPRSTUWabcdefghiklmnopqrstuwx1%] [file ...]\n".to_string()));

        result = shell.func(params.clone()).await.unwrap();
        assert_eq!(expected, result);

        expected.clear();

        // Case fatal error such as command not found
        params.insert("cmd".to_string(), Value::String("hello".to_string()));

        expected.insert("rc".to_string(), Value::Number(Number::from(1)));
        expected.insert("stderr".to_string(), Value::String("No such file or directory (os error 2)".to_string()));

        result = shell.func(params.clone()).await.unwrap();
        assert_eq!(expected, result);
    }
}
