extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;

extern crate json_ops;
use json_ops::JsonOps;

use serde::{Deserialize, Serialize};
//use std::collections::HashMap;
use serde_json::value::Value;
use serde_json::Map;

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


#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
pub struct ConsumerConfig {
    #[serde(default = "default_group_id")]
    pub group_id: String,
    #[serde(default = "default_offset")]
    pub offset: String,
    pub topics: Vec<TopicConfig>
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
pub struct TopicConfig {
    pub name: String,
    pub event: String
}

fn default_group_id() -> String {
    "consumer-group-1".to_string()
}

fn default_offset() -> String {
    "earliest".to_string()
}

// Our plugin implementation
struct KafkaConsumer;

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

    async fn func(&self, params: Map<String, Value>, rx: &Vec<Sender<FlowMessage>>, _tx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        env_logger::init();

        let mut result = PluginExecResult::default();

        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let params_cloned = params.clone();
        let rx_cloned = rx.clone();
        //let result_cloned = result.clone();
        match tokio::spawn(async move {
            return run(params_cloned, &rx_cloned).await;
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

async fn run(params: Map<String, Value>, rx: &Vec<Sender<FlowMessage>>) -> PluginExecResult {
    let mut result = PluginExecResult::default();

    let jops_params = JsonOps::new(Value::Object(params));

    debug!("Job params: {:?}", jops_params);
    let brokers: Vec<String> = match jops_params.get_value_e("brokers") {
        Ok(b) => b,
        Err(e) => {
            result.status = Status::Ko;
            result.error = e.to_string();

            return result;
        },
    };

    let config: ConsumerConfig = match jops_params.get_value_e("consumer") {
        Ok(c) => c,
        Err(e) => {
            result.status = Status::Ko;
            result.error = e.to_string();

            return result;
        },
    };

    let consumer: StreamConsumer  = match ClientConfig::new()
        .set("group.id", config.group_id.clone())
        .set("bootstrap.servers", brokers.join(","))
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", config.offset.clone())
        .set_log_level(RDKafkaLogLevel::Debug)
        .create() {
            Ok(c) => c,
            Err(e) => {
                result.status = Status::Ko;
                result.error = e.to_string();

                return result;
            },
        };

    let topics: Vec<&str> = config.topics.iter().map(|t| t.name.as_ref()).collect();

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

                        let msg = FlowMessage::Json(value);

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
    Box::into_raw(Box::new(KafkaConsumer {}))
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let consumer = KafkaConsumer{};

        let (rx, tx) = bounded::<FlowMessage>(1024);
        let rxs = vec![rx.clone()];
        let txs = vec![tx.clone()];

        let params: Map<String, Value> = Map::new();

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:29092")
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

        let value = json!({"brokers": ["127.0.0.1:29092"], "consumer": {"group_id": "group1", "topics": [{"name": "topic1", "event": "event1"}, {"name": "topic2", "event": "event2"}], "offset": "earliest"}});

        params = value.as_object().unwrap().to_owned();

        tokio::spawn(async move{
            let _ = consumer.func(params.clone(), &rxs, &vec![]).await;
        });

        sleep(Duration::from_millis(1000)).await;

        let result = txs[0].recv().await.unwrap();

        //println!("{:?}", msg);
        assert_eq!(FlowMessage::Json(Value::String(r#"{"message": "hello world"}"#.to_string())), result);
    }
}
