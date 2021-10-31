extern crate flowrunner;

use std::collections::HashMap;
use flowrunner::plugin::Plugin;

use anyhow::{Result, anyhow};
use log::warn;
use async_trait::async_trait;

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

    async fn func(&self, params: HashMap<String, String>) -> Result<HashMap<String, String>>{
        // Handle params
        let cmd = match params.get(&"cmd".to_string()) {
            None => return Err(anyhow!("param `cmd` is not found")),
            Some(c) => c
        };

        println!("cmd: {}", cmd);

        Ok(HashMap::new())
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut dyn Plugin {
    println!("Plugin Shell loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(Shell {}))
}
