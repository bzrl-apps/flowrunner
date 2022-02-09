extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;

extern crate json_ops;
use json_ops::JsonOps;

use serde::{Deserialize, Serialize};
//use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::{json, Map};

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver, bounded};
use tokio::runtime::Runtime;
use async_trait::async_trait;

use log::{info, error, debug};

use rdkafka::{
    config::{ClientConfig, RDKafkaLogLevel},
    consumer::{
        stream_consumer::StreamConsumer,
        Consumer,
        CommitMode,
    },
    message::{Headers, Message},
};


#[derive(Debug ,Serialize, Deserialize, Clone)]
struct Config {
    #[serde(default = "default_group_id")]
    group_id: String,
    #[serde(default = "default_offset")]
    offset: String,
    topics: Vec<String>,
    options: Map<String, Value>,
    #[serde(default = "default_loglevel")]
    log_level: String
}

fn default_group_id() -> String {
    "consumer-group-1".to_string()
}

fn default_offset() -> String {
    "earliest".to_string()
}

fn default_loglevel() -> String {
    "info".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            group_id: "consumer-group-1".to_string(),
            offset: "latest".to_string(),
            topics: vec![],
            options: Map::new(),
            log_level: "info".to_string(),
        }
    }
}

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone)]
struct KafkaConsumer {
    brokers: Vec<String>,
    config: Config,
}

#[async_trait]
impl Plugin for KafkaConsumer {
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

        match jops_params.get_value_e::<Config>("consumer") {
            Ok(c) => self.config = c,
            Err(e) => { return Err(anyhow!(e)); },
        };

        Ok(())
    }

    async fn func(&self, sender: Option<String>, rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        env_logger::init();

        let mut result = PluginExecResult::default();

        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let brokers_cloned = self.brokers.clone();
        let config_cloned = self.config.clone();
        let rx_cloned = rx.clone();
        let sender_cloned = sender.clone();
        match tokio::spawn(async move {
            return run(sender_cloned, brokers_cloned, config_cloned, &rx_cloned).await;
        }).await {
            Ok(r) => r,
            Err(e) => {
                result.status = Status::Ko;
                result.error = e.to_string();

                return result;
            }
        }
    }
}

async fn run(sender: Option<String>, brokers: Vec<String>, config: Config, rx: &Vec<Sender<FlowMessage>>) -> PluginExecResult {
    let mut result = PluginExecResult::default();

    let mut client_config = ClientConfig::new();

    client_config.set("group.id", config.group_id.clone())
                .set("bootstrap.servers", brokers.join(","))
                .set("auto.offset.reset", config.offset.clone());

    match config.log_level.as_str() {
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

    for (k, v) in config.options.iter() {
        match v.as_str() {
            Some(s) => {
                client_config.set(k.as_str(), s);
            },
            None => (),
        }
    }

    let consumer: StreamConsumer  = match client_config
        .set_log_level(RDKafkaLogLevel::Debug)
        .create() {
            Ok(c) => c,
            Err(e) => {
                result.status = Status::Ko;
                result.error = e.to_string();

                return result;
            },
        };

    let topics: Vec<&str> = config.topics.iter().map(|t| t.as_ref()).collect();

    if let Err(e) = consumer.subscribe(&topics) {
        result.status = Status::Ko;
        result.error = e.to_string();

        return result;
    }

    info!("Start receving messages...");
    loop {
        match consumer.recv().await {
            Err(e) => error!("Kafka error: {}", e),
            Ok(m) => {
                debug!("key: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    m.key(), m.topic(), m.partition(), m.offset(), m.timestamp());

                if let Some(headers) = m.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        debug!("Header {:#?}: {:?}", header.0, header.1);
                    }
                }

                match m.payload_view::<str>() {
                    Some(Ok(payload)) => {
                        debug!("Payload received from kafka: {}", payload);
                        let value = match serde_json::from_str(r#payload) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("{}", e.to_string());
                                continue;
                            },
                        };

                        let msg = FlowMessage::JsonWithSender {
                            sender: sender.clone().unwrap_or(brokers.join(",")),
                            source: Some(m.topic().to_string()),
                            value,
                        };

                        for rx1 in rx.iter() {
                            match rx1.send(msg.clone()).await {
                                Ok(()) => {
                                    match consumer.commit_message(&m, CommitMode::Async) {
                                        Ok(_) => (),
                                        Err(e) => {
                                            error!("Error while committing message: {}", e.to_string());
                                        }
                                    }
                                },
                                Err(e) => error!("{}", e.to_string()),
                            };
                        }
                    },
                    Some(Err(e)) => error!("{}", e.to_string()),
                    None => info!("No content received from the topic"),
                };
            }
        };
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    info!("Plugin KafkaConsumer loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(KafkaConsumer::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use json_ops::json_map;
    use rdkafka::{
        config::ClientConfig,
        //message::{Headers, Message},
        producer::future_producer::{FutureProducer, FutureRecord},
    };

    //use std::time::Duration;
    use tokio::time::{sleep, Duration};

    //#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[tokio::test]
    async fn test_func() {
        //env_logger::init();
        let mut consumer = KafkaConsumer::default();

        let (rx, tx) = bounded::<FlowMessage>(1024);
        let rxs = vec![rx.clone()];
        let txs = vec![tx.clone()];

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create().unwrap();

        let msg_bytes = r#"{"message": "hello world"}"#.as_bytes();
        let key_bytes = r#"key1"#.as_bytes();

        let produce_future = producer.send(
            FutureRecord::to("topic1")
                .payload(msg_bytes)
                .key(key_bytes),
                //.headers(OwnedHeaders::new().add("header_key", "header_value")),
            Duration::from_secs(0),
        );

        match produce_future.await {
            Ok(delivery) => println!("Sent delivery status: {:?}", delivery),
            Err((e, _)) => println!("Sent eror: {:?}", e),
        }

        let value = json!({
            "brokers": ["localhost:9092"],
            "consumer": {
                "group_id": "group1",
                "topics": ["topic1"],
                "offset": "earliest",
                "options": {
                    "enable.partition.eof": "false",
                    "session.timeout.ms": "6000",
                    "enable.auto.commit": "true",
                    "auto.commit.interval.ms": "1000",
                    "enable.auto.offset.store": "false"
                }
            }
        });

        let params = value.as_object().unwrap().to_owned();

        consumer.validate_params(params).unwrap();
        tokio::spawn(async move{
            let _ = consumer.func(None, &rxs, &vec![]).await;
        });

        sleep(Duration::from_millis(1000)).await;

        let result = txs[0].recv().await.unwrap();

        //println!("{:?}", msg);
        assert_eq!(FlowMessage::JsonWithSender{sender: "localhost:9092".to_string(), source: Some("topic1".to_string()), value: Value::Object(json_map!("message" => Value::String("hello world".to_string())))}, result);
    }
}
