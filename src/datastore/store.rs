use anyhow::{anyhow, Result};
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};

use crate::datastore::store_rocksdb::RocksDB;

pub type BoxStore = Box<dyn Store + Send + Sync>;

pub trait Store: StoreClone {
    fn list_namespaces(&self) -> Result<Vec<String>>;
    fn set(&self, ns: &str, k: &str, v: &str) -> Result<()>;
    fn get(&self, ns: &str, k: &str) -> Result<String>;
    fn delete(&self, ns: &str, k: &str) -> Result<()>;
    fn find(&self, ns: &str, k: &str) -> Result<Map<String, Value>>;
}

pub trait StoreClone {
    fn clone_box(&self) -> BoxStore;
}

impl<T> StoreClone for T
where
    T: 'static + Store + Send + Sync + Clone,
{
    fn clone_box(&self) -> BoxStore {
        Box::new(self.clone())
    }
}

impl Clone for BoxStore {
    fn clone(&self) -> BoxStore {
        self.clone_box()
    }
}

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
    pub prefix_len: Option<usize>,
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
