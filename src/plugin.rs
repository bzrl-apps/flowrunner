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

use glob::glob;

use anyhow::Result;
use log::info;

use crate::config::Config;

// The trait that must be implemented by plugins to allow them to handle
// commands.
#[async_trait]
pub trait Plugin {
    fn get_name(&self) -> String;
    fn get_version(&self) -> String;
    fn get_description(&self) -> String;
    async fn func(&self, params: Map<String, Value>) -> Result<Map<String, Value>>;
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

    pub async fn exec_plugin(&self, name: &str, params: Map<String, Value>) -> Result<Map<String, Value>> {
        let plugin_lib = self.plugins.get(&"shell".to_string()).unwrap();
        let api = unsafe { PluginApi::load(&plugin_lib.lib) }.expect("Could not load symboles");
        let plugin = unsafe { Box::from_raw((api.get_plugin)()) };

        plugin.func(params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;
    use serde_json::value::Value as jsonValue;

    #[tokio::test]
    //#[test]
    async fn test_load_plugins() {
        PluginRegistry::load_plugins("target/debug");

        let registry = PluginRegistry::get().lock().unwrap();

        let mut params = Map::new();
        params.insert("cmd".to_string(), jsonValue::String("ls -la".to_string()));

        let res = registry.exec_plugin("shell", params).await.unwrap();
        println!("{:?}", res)
    }
}
