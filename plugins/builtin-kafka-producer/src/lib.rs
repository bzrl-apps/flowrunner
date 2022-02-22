extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;

extern crate json_ops;
use json_ops::JsonOps;

use serde::{Deserialize, Serialize};
//use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::Map;

use std::time::Duration;
use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use tokio::runtime::Runtime;
use async_trait::async_trait;

use log::{info, error, debug};

use rdkafka::{
    config::{ClientConfig, RDKafkaLogLevel},
    producer::future_producer::{FutureProducer, FutureRecord},
};

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct KafkaProducer {
    brokers: Vec<String>,
    options: Map<String, Value>,
    messages: Vec<KafkaMessage>,
    #[serde(default = "default_loglevel")]
    log_level: String
}

fn default_loglevel() -> String {
    "info".to_string()
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct KafkaMessage {
    topic: String,
    key: String,
    message: String,
}

#[async_trait]
impl Plugin for KafkaProducer {
    fn get_name(&self) -> String {
        env!("CARGO_PKG_NAME").to_string()
    }

    fn get_version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }

    fn get_description(&self) -> String {
        env!("CARGO_PKG_DESCRIPTION").to_string()
    }

    fn get_params(&self) -> Map<String, Value> {
        let params: Map<String, Value> = Map::new();

        params
    }

    fn validate_params(&mut self, params: Map<String, Value>) -> Result<()> {
        let jops_params = JsonOps::new(Value::Object(params));

        match jops_params.get_value_e::<Vec<String>>("brokers") {
            Ok(b) => self.brokers = b,
            Err(e) => { return Err(anyhow!(e)); },
        };

        match jops_params.get_value_e::<Map<String, Value>>("options") {
            Ok(o) => self.options = o,
            Err(_) => self.options = Map::new(),
        };

        match jops_params.get_value_e::<Vec<KafkaMessage>>("messages") {
            Ok(m) => {
                for m in m.iter() {
                    if m.topic == "" || m.message == "" {
                        return Err(anyhow!("Topic or message must not be empty!"));
                    }
                }

                self.messages = m;
            }
            Err(e) => { return Err(anyhow!(e)); },
        };

        match jops_params.get_value_e::<String>("log_level") {
            Ok(l) => self.log_level = l,
            Err(_) => self.log_level = "info".to_string(),
        };

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}

    async fn func(&self, _sender: Option<String>, _rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
       let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", self.brokers.join(","));

        match self.log_level.as_str() {
            "debug" => client_config.set_log_level(RDKafkaLogLevel::Debug),
            "info" => client_config.set_log_level(RDKafkaLogLevel::Info),
            "notice" => client_config.set_log_level(RDKafkaLogLevel::Notice),
            "warning" => client_config.set_log_level(RDKafkaLogLevel::Warning),
            "error" => client_config.set_log_level(RDKafkaLogLevel::Error),
            "critical" => client_config.set_log_level(RDKafkaLogLevel::Critical),
            "alert" => client_config.set_log_level(RDKafkaLogLevel::Alert),
            "emerg" => client_config.set_log_level(RDKafkaLogLevel::Emerg),
            _ => client_config.set_log_level(RDKafkaLogLevel::Info),
        };

        for (k, v) in self.options.iter() {
            match v.as_str() {
                Some(s) => {
                    client_config.set(k.as_str(), s);
                },
                None => (),
            }
        }

        let producer: FutureProducer  = match client_config
            .create() {
                Ok(c) => c,
                Err(e) => { return_plugin_exec_result_err!(result, e.to_string()); },
            };


        for msg in self.messages.clone().iter() {
            info!("Sending message {:?}", msg);

            let mut fr = FutureRecord::to(msg.topic.as_str())
                    .payload(msg.message.as_bytes());

            if msg.key != "" {
                   fr =  fr.key(msg.key.as_bytes());
            }

            let produce_future = producer.send(
                fr,
                //.headers(OwnedHeaders::new().add("header_key", "header_value")),
                Duration::from_secs(0),
            );

            match produce_future.await {
                Ok(delivery) => {
                    debug!("Kafka producer sent delivery status: {:?}", delivery);
                }
                Err((e, _)) => {
                    error!("Kafka producer sent error: {:?}", e);
                    return_plugin_exec_result_err!(result, e.to_string());
                }
            };
        }

        result.status = Status::Ok;
        result
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    info!("Plugin KafkaProducer loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(KafkaProducer::default()))
}
