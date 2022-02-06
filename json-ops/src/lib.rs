use core::slice;
use std::any::TypeId;

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
//use std::collections::HashMap;
use serde_json::Value;
use serde_json::Map;
use serde_json::json;

use anyhow::{anyhow, Result};

#[macro_export]
macro_rules! json_map {
    ($( $key:expr => $val:expr), *) => {
        {
            let mut map = serde_json::Map::new();
            $(map.insert($key.to_string(), $val); )*

            map
        }
    }
}

#[derive(Debug, Default)]
pub struct JsonOps {
    value: Value,
}

impl JsonOps {
    pub fn new(value: Value) -> Self {
        JsonOps { value }
    }

    pub fn get_json_value(&self, path: &str) -> Value {
        self.get_value_by_path(path)
    }

    pub fn get_value<T: DeserializeOwned + Default>(&self, path: &str) -> T {
        let v = self.get_value_by_path(path);

        match serde_json::from_value(v) {
            Ok(v) => v,
            Err(_) => T::default(),
        }
    }

    pub fn get_value_e<T: DeserializeOwned + Default>(&self, path: &str) -> Result<T> {
        let v = self.get_value_by_path(path);

        match serde_json::from_value(v) {
            Ok(v) => Ok(v),
            Err(e) => Err(anyhow!(e)),
        }
    }

    fn get_value_by_path(&self, path: &str) -> Value {
        let slice_path: Vec<&str> = path.split('.').collect();
        let mut val = self.value.clone();

        for k in slice_path.iter() {
            val = match k.parse::<usize>() {
                Ok(n) => val[n].clone(),
                Err(_) => val[k].clone(),
            };

            if val == Value::Null {
                return val;
            }
        }

        val
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
    struct Topic {
        name: String,
        event: String
    }

    #[test]
    fn test_get_value() {
        let value = json!({"sources": [{"name": "kafka1", "plugin": "kafka", "params": {"brokers": ["127.0.0.1:9092"], "consumer": {"group_id": "group1", "topics": [{"name": "topic1", "event": "event1"}, {"name": "topic2", "event": "event2"}], "offset": "earliest"}}}]});

        let jop = JsonOps::new(value);

        let offset: String = jop.get_value("sources.0.params.consumer.offset");
        let brokers_fake: Vec<String> = jop.get_value("sources.0.params.broker");
        let brokers: Vec<String> = jop.get_value("sources.0.params.brokers");

        let topics: Topic = jop.get_value("sources.0.params.consumer.topics.1");

        assert_eq!("earliest".to_string(), offset);
        assert_eq!(Vec::<String>::new(), brokers_fake);
        assert_eq!(vec!["127.0.0.1:9092"], brokers);
        assert_eq!(Topic{name: "topic2".to_string(), event: "event2".to_string()}, topics);
    }
}
