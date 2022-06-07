//use dlopen::wrapper::{Container, WrapperApi};
//use dlopen_derive::WrapperApi;
use dlopen::symbor::{Symbol, Library, SymBorApi};
use dlopen_derive::SymBorApi;
//use once_cell::sync::OnceCell;
use lazy_static::lazy_static;
//use std::sync::Mutex;

use core::panic;
//use futures::lock::Mutex;
use std::sync::Mutex;

use async_trait::async_trait;

use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::Map;
use serde::{Deserialize, Serialize};

//use tokio::sync::mpsc::*;
use async_channel::{Sender, Receiver};

use glob::glob;
use std::fmt;

use anyhow::Result;
use log::{info, debug};

use crate::message::Message as FlowMessage;
use crate::datastore::store::BoxStore;

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

#[macro_export]
macro_rules! return_plugin_exec_result_err {
    ($result:expr, $err:expr) => {
        {
            $result.status = Status::Ko;
            $result.error = $err;

            return $result;
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
    fn get_params(&self) -> Map<String, Value>;
    fn set_datastore(&mut self, datastore: Option<BoxStore>);
    fn validate_params(&mut self, params: Map<String, Value>) -> Result<()>;
    //fn set_kvstore(&self, store: dyn KVStore);
    async fn func(&self, sender: Option<String>, rx: &Vec<Sender<FlowMessage>>, tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult;
}

pub type BoxPlugin = Box<(dyn Plugin + Sync + Send + 'static)>;

struct PluginLib {
    path: String,
    lib: Library,
}

impl fmt::Debug for PluginLib {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("PluginLib")
           .field("path", &self.path)
           .finish()
    }
}

#[derive(SymBorApi)]
struct PluginApi<'a> {
    // The plugin library must implement this function and return a raw pointer
    // to a Plugin struct.
    get_plugin: Symbol<'a, unsafe extern fn() -> *mut (dyn Plugin + Send + Sync)>,
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

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PluginExecResult {
    #[serde(default)]
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

    pub async fn load_plugins(dir: &str) {
        let mut pr = PLUGIN_REGISTRY.lock().unwrap();

        let os_type = sys_info::os_type().unwrap();

        let pattern = match os_type.to_lowercase().as_str() {
            "linux" => dir.to_owned() + "/" + "*.so",
            "darwin" => dir.to_owned() + "/" + "*.dylib",
            _ => panic!("OS type {} not supported", os_type),
        };

        for entry in glob(pattern.as_str()).expect("Failed to read files *.so in the specified plugin directory") {
            match entry {
                Ok(path) => {
                    info!("Loading plugin: {:?}", path.display());

                    let p = path.display().to_string().to_owned();
                    let lib = Library::open(p).expect("Could not open the library");
                    let api = unsafe { PluginApi::load(&lib) }.expect("Could not load symboles");
                    let plugin = unsafe { Box::from_raw((api.get_plugin)()) };

                    info!("Inserting {} into plugin registry", plugin.get_name());
                    pr.plugins.insert(plugin.get_name(), PluginLib{
                        path: path.display().to_string(),
                        lib,
                    });

                },
                Err(e) => panic!("Error to load plugin: {:?}", e),
            }
        }
    }

    pub fn get_plugin(name: &str) -> Option<BoxPlugin> {
        let registry = PluginRegistry::get().lock().unwrap();

        debug!("Searching plugin {} in the plugin registry: {:?}", name, registry.plugins);
        if let Some(plugin_lib) = registry.plugins.get(name) {
            let api = unsafe { PluginApi::load(&plugin_lib.lib) }.expect("Could not load symboles");
            return unsafe { Some(Box::from_raw((api.get_plugin)())) };
        }

        None
    }

    //pub fn debug(&self) {
        //info!("Plugin registry: {:?}", self.plugins);
    //}
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Number};
    use serde_json::value::Value as jsonValue;

    #[tokio::test]
    //#[test]
    async fn test_load_plugins() {
        let _ =  env_logger::try_init();
        PluginRegistry::load_plugins("target/debug").await;


        let mut params = Map::new();
        params.insert("cmd".to_string(), jsonValue::String("ls -la".to_string()));

        let mut plugin = PluginRegistry::get_plugin("builtin-shell").unwrap();
        plugin.validate_params(params).unwrap();

        let _ = plugin.func(None, &vec![], &vec![]).await;

        //println!("{:?}", res)
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
