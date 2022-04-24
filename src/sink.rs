//use crate::plugin::Status;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;

//use tokio::sync::mpsc::*;
//use futures::lock::Mutex;
use async_channel::*;

use anyhow::{Result, anyhow};

use evalexpr::*;

use log::*;

use crate::plugin::{PluginRegistry, Status as PluginStatus};
use crate::message::Message as FlowMessage;
use crate::utils::*;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Sink {
    #[serde(default)]
	pub name: String,
	#[serde(default)]
	pub r#if: Option<String>,
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

        if !self.tx.is_empty() {
            loop {
                match self.tx[0].recv().await {
                    // Add message received as data in job context
                    Ok(msg) => {
                        match msg {
                            FlowMessage::JsonWithSender{ uuid: id, sender: s, source: src, value: v } => {
                                self.context.insert("uuid".to_string(), Value::String(id));
                                self.context.insert("sender".to_string(), Value::String(s));
                                self.context.insert("source".to_string(), Value::String(src.unwrap_or_else(|| "".to_string())));
                                self.context.insert("data".to_string(), v);
                            },
                            _ => {
                                error!("Message received is not Message::JsonWithSender type");
                                continue;
                            },
                       }

                        if let Err(e) = self.exec_plugin().await {
                            error!("{e}");
                            continue;
                        }
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

        if !self.render_template(&mut s)? {
            return Ok(());
        }

        match PluginRegistry::get_plugin(&s.plugin) {
            Some(mut plugin) => {
                plugin.validate_params(s.params.clone())?;

                let rx_cloned = vec![];
                let tx_cloned = vec![];
                //let result_cloned = result.clone();
                tokio::spawn(async move {
                    let res = plugin.func(None, &rx_cloned, &tx_cloned).await;
                    if res.status == PluginStatus::Ko {
                        error!("plugin func error: {}", res.error);
                    }
                });
            },
            None => error!("No plugin {} found", self.plugin),
        }

        Ok(())
    }

    fn render_template(&self, sink: &mut Sink) -> Result<bool> {
        let mut data: Map<String, Value> = Map::new();
        let component = sink.name.as_str();

        data.insert("context".to_string(), Value::from(self.context.clone()));

        expand_env_map(&mut data);

        if let Some(mut txt) = sink.r#if.clone() {
            // Expand job if condition
            render_text_template(component, &mut txt, &data)?;

            match eval_boolean(&txt) {
                Ok(b) => {
                    if !b {
                        debug!("{}", format!("sink ignored: {}, if: {:?}, eval: {:?}", component, txt, b));
                        return Ok(false);
                    }

                    return Ok(true)
                },
                Err(e) => return Err(anyhow!(e)),
            };
        }

        // Expand task's params
        for (n, v) in sink.params.clone().into_iter() {
            sink.params.insert(n.to_string(), render_param_template(sink.name.as_str(), &n, &v, &data)?);
        }

        Ok(true)
    }
}
