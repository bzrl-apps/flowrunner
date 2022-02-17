use anyhow::{anyhow, Result};
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};

use crate::datastore::store_rocksdb::RocksDB;

pub trait Store {
    fn list_namespaces(&self) -> Result<Vec<String>>;
    fn save(&self, ns: &str, k: &str, v: &str) -> Result<()>;
    fn find(&self, ns: &str, k: &str) -> Result<String>;
    fn delete(&self, ns: &str, k: &str) -> Result<()>;
}

pub type BoxStore = Box<dyn Store + Send + Sync>;

/// Store configuration
///
/// The configuration is defined for a local file store or a remote store. `conn_str` can be an
/// absolute path to a local file or a tcp address to a remote server. If the server supports TTL,
/// we can define it with the attribute `ttl`. `Namespace` can be used to isolate data.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoreConfig {
    pub kind: String,
    pub conn_str: String,
    pub options: Map<String, Value>,
    pub ttl: u64,
    pub namespaces: Vec<StoreNamespace>
}

/// Store namespace
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoreNamespace {
    pub name: String,
    pub options: Map<String, Value>
}

impl StoreConfig {
    pub fn new_store(&self) -> Result<BoxStore> {
        let db: BoxStore = match self.kind.as_str() {
            "rocksdb" => Box::new(RocksDB::init(&self)),
            _ => return Err(anyhow!("{}", format!("Datastore's kind {} not supported!", self.kind))),
        };

        Ok(db)
    }
}
