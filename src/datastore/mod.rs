use anyhow::Result;
use std::time::Duration;
use serde_json::{Value, Map};

mod rocksdb;

/// Store configuration
///
/// The configuration is defined for a local file store or a remote store. `conn_str` can be an
/// absolute path to a local file or a tcp address to a remote server. If the server supports TTL,
/// we can define it with the attribute `ttl`. `Namespace` can be used to isolate data.
pub struct StoreConfig {
    conn_str: String,
    options: Map<String, Value>,
    ttl: Option<Duration>,
    namespaces: Vec<StoreNamespace>
}

/// Store namespace
pub struct StoreNamespace {
    name: String,
    options: Map<String, Value>
}

pub trait Store {
    fn list_namespaces(&self) -> Result<Vec<String>>;
    fn save(&self, ns: &str, k: &str, v: &str) -> Result<()>;
    fn find(&self, ns: &str, k: &str) -> Result<String>;
    fn delete(&self, ns: &str, k: &str) -> Result<()>;
}
