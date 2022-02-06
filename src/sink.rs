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
use crate::utils::*;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Sink {
    #[serde(default)]
	pub name: String,
    #[serde(default)]
    pub plugin: String,
    #[serde(default)]
	pub params: Map<String, Value>,

    #[serde(default)]
	pub context: Map<String, Value>,
    #[serde(skip_serializing, skip_deserializing)]
	pub rx: Vec<Sender<FlowMessage>>,
    #[serde(skip_serializing, skip_deserializing)]
	pub tx: Vec<Receiver<FlowMessage>>
}

impl PartialEq for Sink {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name &&
        self.plugin == other.plugin &&
        self.params == other.params &&
        self.context == other.context
    }
}

impl Sink {
    pub async fn run(&mut self) -> Result<()> {
        info!("SINK RUN STARTED: name {}, plugin {}, params: {:?}, nb tx: {}", self.name, self.plugin, self.params, self.tx.len());

        if self.tx.len() > 0 {
            loop {
                match self.tx[0].recv().await {
                    // Add message received as data in job context
                    Ok(msg) => {
                        match msg {
                            FlowMessage::Json(v) => { self.context.insert("data".to_string(), v); },
                            _ => {
                                error!("Message received is not Message::Json type");
                                continue;
                            },
                       }

                        self.exec_plugin().await?;
                    },
                    Err(e) => { error!("{}", e.to_string()); break; },
                }
            }
        } else {
            self.exec_plugin().await?;
        }

        Ok(())
    }

    async fn exec_plugin(&self) -> Result<()> {
        let mut s = self.clone();

        self.render_sink_template(&mut s)?;

        match PluginRegistry::get_plugin(&s.plugin) {
            Some(mut plugin) => {
                plugin.validate_params(s.params.clone())?;

                let rx_cloned = vec![];
                let tx_cloned = vec![];
                //let result_cloned = result.clone();
                tokio::spawn(async move {
                    let res = plugin.func(&rx_cloned, &tx_cloned).await;
                    if res.status == PluginStatus::Ko {
                        error!("plugin func error: {}", res.error);
                    }
                });
            },
            None => error!("No plugin {} found", self.plugin),
        }

        Ok(())
    }

    fn render_sink_template(&self, sink: &mut Sink) -> Result<()> {
        let mut data: Map<String, Value> = Map::new();

        data.insert("context".to_string(), Value::from(self.context.clone()));

        expand_env_map(&mut data);

        // Expand task's params
        for (n, v) in sink.params.clone().into_iter() {
            sink.params.insert(n.to_string(), render_param_template(sink.name.as_str(), &n, &v, &data)?);
        }

        Ok(())
    }
}
