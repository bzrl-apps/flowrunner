//use crate::plugin::Status;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;

//use tokio::sync::mpsc::*;
//use futures::lock::Mutex;
use async_channel::*;

use anyhow::{Result, anyhow};

use log::{info, debug, error, warn};

use crate::plugin::{PluginRegistry, Status as PluginStatus};
use crate::message::Message as FlowMessage;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Source {
    #[serde(default)]
	pub name: String,
    #[serde(default)]
    pub plugin: String,
    #[serde(default)]
	pub params: Map<String, Value>,
    #[serde(skip_serializing, skip_deserializing)]
	pub rx: Vec<Sender<FlowMessage>>,
    #[serde(skip_serializing, skip_deserializing)]
	pub tx: Vec<Receiver<FlowMessage>>
}

impl PartialEq for Source {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name &&
        self.plugin == other.plugin &&
        self.params == other.params
    }
}

impl Source {
    pub async fn run(&mut self) -> Result<()> {
        info!("SOURCE RUN STARTED: name {}, plugin {}, params: {:?}, nb rx: {}", self.name, self.plugin, self.params, self.rx.len());

        match PluginRegistry::get_plugin(&self.plugin) {
            Some(mut plugin) => {
                plugin.validate_params(self.params.clone())?;

                let rx_cloned = self.rx.clone();
                let tx_cloned = vec![];
                let sender = self.name.clone();
                //let result_cloned = result.clone();
                tokio::spawn(async move {
                    let res = plugin.func(Some(sender), &rx_cloned, &tx_cloned).await;
                    if res.status == PluginStatus::Ko {
                        error!("{}", res.error);
                    }
                });
            },
            None => error!("No plugin {} found", self.plugin),
        }

        Ok(())
    }
}
