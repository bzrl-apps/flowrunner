use dlopen::wrapper::{Container, WrapperApi};
use dlopen_derive::WrapperApi;

use std::collections::HashMap;
use glob::glob;

use anyhow::Result;
use log::warn;

// The trait that must be implemented by plugins to allow them to handle
// commands.
pub trait Plugin {
    fn get_name(&self) -> String;
    fn get_version(&self) -> String;
    fn get_description(&self) -> String;
    fn func(&self, params: HashMap<String, String>) -> Result<HashMap<String, String>>;
}

#[derive(WrapperApi)]
struct PluginApi {
    // The plugin library must implement this function and return a raw pointer
    // to a Plugin struct.
    get_plugin: extern fn() -> *mut dyn Plugin,
}

// PluginRegistry is a registry for plugins
pub struct PluginRegistry {
	pub plugins: HashMap<String, Box<dyn Plugin>>
}

impl PluginRegistry {
    pub fn new() -> Self {
        PluginRegistry {
            plugins: HashMap::new()
        }
    }

    pub fn load_plugins(&self, dir: &str) -> Result<()> {
        let pattern = dir.to_owned() + "/" + "*.dylib";

        for entry in glob(pattern.as_str()).expect("Failed to read files *.so in the specified plugin directory") {
            match entry {
                Ok(path) => {
                    println!("{:?}", path.display());

                    let p = path.display().to_string().to_owned();
                    //Load the plugin by name from the plugins directory
                    let plugin_api_wrapper: Container<PluginApi> = unsafe { Container::load(p) }.unwrap();
                    let plugin = unsafe { Box::from_raw(plugin_api_wrapper.get_plugin()) };

                    
                    // Give the plugin a chance to handle the command
                    let mut params = HashMap::<String, String>::new();
                    params.insert("cmd".to_string(), "ls -la".to_string());

                    plugin.func(params);
                },
                Err(e) => println!("{:?}", e),
            }
        }
    // Load the plugin by name from the plugins directory
        //let plugin_api_wrapper: Container<PluginApi> = unsafe { Container::load("plugins/libplugin1.so") }.unwrap();
        //let plugin = unsafe { Box::from_raw(plugin_api_wrapper.get_plugin()) };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_plugins() {
        let registry = PluginRegistry::new();
        let _ = registry.load_plugins("target/debug");
    }
}
