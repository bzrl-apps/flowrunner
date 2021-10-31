//use dlopen::wrapper::{Container, WrapperApi};
//use dlopen_derive::WrapperApi;
use dlopen::symbor::{Symbol, Library, SymBorApi};
use dlopen_derive::SymBorApi;
//use once_cell::sync::OnceCell;
//use lazy_static::lazy_static;

use std::any::Any;
use async_trait::async_trait;

use std::collections::HashMap;

use glob::glob;

use anyhow::Result;
use log::warn;

use crate::config::Config;

// The trait that must be implemented by plugins to allow them to handle
// commands.
#[async_trait]
pub trait Plugin {
    fn get_name(&self) -> String;
    fn get_version(&self) -> String;
    fn get_description(&self) -> String;
    async fn func(&self, params: HashMap<String, String>) -> Result<HashMap<String, String>>;
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

// PluginRegistry is a registry for plugins
pub struct PluginRegistry {
	plugins: HashMap<String, PluginLib>
}

impl PluginRegistry {
    pub fn new() -> Self {
        PluginRegistry{
            plugins: HashMap::new(),
        }
    }

    pub fn load_plugins(&mut self, dir: &str) -> Result<()> {
        let pattern = dir.to_owned() + "/" + "*.dylib";

        for entry in glob(pattern.as_str()).expect("Failed to read files *.so in the specified plugin directory") {
            match entry {
                Ok(path) => {
                    println!("Loading plugin: {:?}", path.display());

                    let p = path.display().to_string().to_owned();
                    //Load the plugin by name from the plugins directory
                    //let plugin_api_wrapper: Container<PluginApi> = unsafe { Container::load(p) }.unwrap();
                    //let plugin = unsafe { Box::from_raw(plugin_api_wrapper.get_plugin()) };
                    let lib = Library::open(p).expect("Could not open the library");
                    let api = unsafe { PluginApi::load(&lib) }.expect("Could not load symboles");
                    let plugin = unsafe { Box::from_raw((api.get_plugin)()) };

                    self.plugins.insert(plugin.get_name(), PluginLib{
                        path: path.display().to_string(),
                        lib,
                    });

                },
                Err(e) => println!("Error to load plugin: {:?}", e),
            }
        }

        Ok(())
    }

    pub async fn exec_plugin(&self, name: &str, params: HashMap<String, String>) -> Result<HashMap<String, String>> {
        let plugin_lib = self.plugins.get(&"shell".to_string()).unwrap();
        let api = unsafe { PluginApi::load(&plugin_lib.lib) }.expect("Could not load symboles");
        let plugin = unsafe { Box::from_raw((api.get_plugin)()) };

        plugin.func(params).await
    }
    // Spawn plugins and wait for all futures
    //pub async fn spawn_plugins(&'static self) {
        //let mut futures = Vec::new();
        //for (_id, plugin) in &self.plugins {
            //futures.push(tokio::spawn(async move {
                //let params = HashMap::new();
                //plugin.func(params).await;
            //}));
        //}
        //futures::future::join_all(futures).await;
    //}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    //#[test]
    async fn test_load_plugins() {
        let mut registry = PluginRegistry::new();
        let _ = registry.load_plugins("target/debug");

        //let mut params = HashMap::<String, String>::new();
        //params.insert("cmd".to_string(), "ls -la".to_string());

        //registry.exec_plugin("shell", params).await;

    }
}
