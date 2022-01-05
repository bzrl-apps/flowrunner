//use flowrunner::plugin::Status;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;

use anyhow::{Result, anyhow};

use log::{info, debug, error, warn};

use crate::plugin::{PluginRegistry, PluginExecResult, Status as PluginStatus};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Source {
    #[serde(default)]
	pub name: String,
    #[serde(default)]
    pub plugin: String,
    #[serde(default)]
	pub params: Map<String, Value>,
}

impl Source {
    pub async fn run(&mut self) -> Result<()> {
        info!("SOURCE RUN STARTED: name {}, plugin {}, params: {:?}", self.name, self.plugin, self.params);

        let registry = PluginRegistry::get().lock().unwrap();

        let res = registry.exec_plugin(&self.plugin, self.params.clone()).await;
        if res.status == PluginStatus::Ko {
            return Err(anyhow!(res.error));
        }

        Ok(())
    }
}
