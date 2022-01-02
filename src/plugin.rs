//use dlopen::wrapper::{Container, WrapperApi};
//use dlopen_derive::WrapperApi;
use dlopen::symbor::{Symbol, Library, SymBorApi};
use dlopen_derive::SymBorApi;
//use once_cell::sync::OnceCell;
use lazy_static::lazy_static;
use std::sync::Mutex;

use async_trait::async_trait;

use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::Map;
use serde::{Deserialize, Serialize};

use glob::glob;

use anyhow::Result;
use log::info;

use crate::config::Config;

#[macro_export]
macro_rules! plugin_exec_result {
    ($status:expr, $err:expr, $( $key:expr => $val:expr), *) => {
        {
            let mut result = PluginExecResult {
                status: $status,
                error: $err.to_string(),
                output: serde_json::Map::new()
            };

            $( result.output.insert($key.to_string(), $val); )*
            result
        }
    }
}

// The trait that must be implemented by plugins to allow them to handle
// commands.
#[async_trait]
pub trait Plugin {
    fn get_name(&self) -> String;
    fn get_version(&self) -> String;
    fn get_description(&self) -> String;
    async fn func(&self, params: Map<String, Value>) -> PluginExecResult;
}

struct PluginLib {
    path: String,
    lib: Library,
}

#[derive(SymBorApi)]
struct PluginApi<'a> {
    // The plugin library must implement this function and return a raw pointer
    // to a Plugin struct.
    get_plugin: Symbol<'a, unsafe extern fn() -> *mut dyn Plugin>,
}

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Status {
    Ko = 0,
    Ok = 1,
}

// Implement trait Default for Status
impl Default for Status {
    fn default() -> Self {
        Status::Ko
    }
}

impl Status {
    fn ko() -> Self { Status::Ko }
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PluginExecResult {
    #[serde(default = "Status::ko")]
	pub status: Status,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub output: Map<String, Value>
}

lazy_static! {
    static ref PLUGIN_REGISTRY: Mutex<PluginRegistry> = Mutex::new(PluginRegistry{
        plugins: HashMap::new(),
    });
}

// PluginRegistry is a registry for plugins
pub struct PluginRegistry {
	plugins: HashMap<String, PluginLib>
}

impl PluginRegistry {
    pub fn get() -> &'static Mutex<Self> {
        &PLUGIN_REGISTRY
    }

    pub fn load_plugins(dir: &str) {
        let mut pr = PLUGIN_REGISTRY.lock().unwrap();

        let pattern = dir.to_owned() + "/" + "*.dylib";

        for entry in glob(pattern.as_str()).expect("Failed to read files *.so in the specified plugin directory") {
            match entry {
                Ok(path) => {
                    info!("Loading plugin: {:?}", path.display());

                    let p = path.display().to_string().to_owned();
                    let lib = Library::open(p).expect("Could not open the library");
                    let api = unsafe { PluginApi::load(&lib) }.expect("Could not load symboles");
                    let plugin = unsafe { Box::from_raw((api.get_plugin)()) };

                    pr.plugins.insert(plugin.get_name(), PluginLib{
                        path: path.display().to_string(),
                        lib,
                    });

                },
                Err(e) => panic!("Error to load plugin: {:?}", e),
            }
        }
    }

    pub async fn exec_plugin(&self, name: &str, params: Map<String, Value>) -> PluginExecResult {
        let plugin_lib = match self.plugins.get(name) {
            Some(p) => p,
            None => return plugin_exec_result!(Status::Ko, "Plugin ".to_string() + name + " not found",),
        };

        let api = unsafe { PluginApi::load(&plugin_lib.lib) }.expect("Could not load symboles");
        let plugin = unsafe { Box::from_raw((api.get_plugin)()) };

        plugin.func(params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Number};
    use serde_json::value::Value as jsonValue;

    #[tokio::test]
    //#[test]
    async fn test_load_plugins() {
        PluginRegistry::load_plugins("target/debug");

        let registry = PluginRegistry::get().lock().unwrap();

        let mut params = Map::new();
        params.insert("cmd".to_string(), jsonValue::String("ls -la".to_string()));

        let res = registry.exec_plugin("shell", params).await;
        println!("{:?}", res)
    }

    #[test]
    fn test_macro() {
        let mut expected = PluginExecResult {
            status: Status::Ok,
            error: "helloworld".to_string(),
            output: Map::new()
        };

        expected.output.insert("rc".to_string(), jsonValue::Number(Number::from_f64(17.05).unwrap()));
        expected.output.insert("stdout".to_string(), jsonValue::String("task1".to_string()));

        let macro_res = plugin_exec_result!(
            Status::Ok,
            "helloworld",
            "rc" => jsonValue::Number(Number::from_f64(17.05).unwrap()),
            "stdout" => jsonValue::String("task1".to_string()));

        assert_eq!(expected, macro_res)
    }
}
