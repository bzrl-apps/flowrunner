use rocksdb::{DB, Options, ColumnFamilyDescriptor};
use std::{sync::Arc, collections::HashMap};

use serde_json::{Map, Value};

use anyhow::{Result, anyhow};

use log::debug;

use crate::datastore::{Store, StoreConfig};

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
    db_opts: Options,
    ns_opts: HashMap<String, Options>
}

impl RocksDB {
    fn init(config: &StoreConfig) -> Self {
        let mut cfs: Vec<ColumnFamilyDescriptor> = Vec::new();
        let mut ns_opts: HashMap<String, Options> = HashMap::new();

        for ns in config.namespaces.iter() {
            let cf_opts = set_opts(ns.options.clone());
            cfs.push(ColumnFamilyDescriptor::new(ns.name.clone(), cf_opts.clone()));
            ns_opts.insert(ns.name.clone(), cf_opts.clone());
        }

        let db_opts = set_opts(config.options.clone());

        let db = match config.ttl {
            Some(d) => DB::open_cf_descriptors_with_ttl(&db_opts, &config.conn_str, cfs, d).unwrap(),
            None => DB::open_cf_descriptors(&db_opts, &config.conn_str, cfs).unwrap(),
        };

        RocksDB {
            db: Arc::new(db),
            db_opts,
            ns_opts,
        }
    }
}

impl Store for RocksDB {
    fn list_namespaces(&self) -> Result<Vec<String>> {
        match DB::list_cf(&self.db_opts, self.db.path()) {
            Ok(v) => Ok(v),
            Err(e) => Err(anyhow!(e)),
        }
    }

    fn save(&self, ns: &str, k: &str, v: &str) -> Result<()> {
        if let Some(cf) = self.db.cf_handle(ns) {
            if let Err(e) = self.db.put_cf(cf, k.as_bytes(), v.as_bytes()) {
                return Err(anyhow!(e));
            }
        }

        Err(anyhow!("{}", format!("{} not found", ns)))
    }

    fn find(&self, ns: &str, k: &str) -> Result<String> {
        if let Some(cf) = self.db.cf_handle(ns) {
            match self.db.get_cf(cf, k.as_bytes()) {
                Ok(Some(v)) => {
                    let result = String::from_utf8(v).unwrap();

                    debug!("Finding '{}' returns '{}'", k, result);

                    return Ok(result);
                },
                Ok(None) => {
                    println!("Finding '{}' returns None", k);

                    return Ok("".to_string());
                },
                Err(e) => {
                    return Err(anyhow!("{}", format!("Error retrieving value for {}: {}", k, e)));
                }
            }
        }

        Err(anyhow!("{}", format!("{} not found", ns)))
    }

    fn delete(&self, ns: &str, k: &str) -> Result<()> {
        if let Some(cf) = self.db.cf_handle(ns) {
            if let Err(e) = self.db.delete_cf(cf, k.as_bytes()) {
                return Err(anyhow!(e));
            }
        }

        Err(anyhow!("{}", format!("{} not found", ns)))
    }
}

fn set_opts(options: Map<String, Value>) -> Options {
    let mut opts = Options::default();

    if let Some(o) = options.get("create_if_missing") {
        opts.create_if_missing(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("set_max_open_files") {
        opts.set_max_open_files(o.as_i64().unwrap_or(-1) as i32);
    }

    if let Some(o) = options.get("set_use_fsync") {
        opts.set_use_fsync(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("set_bytes_per_sync") {
        opts.set_bytes_per_sync(o.as_u64().unwrap_or(0));
    }

    if let Some(o) = options.get("optimize_for_point_lookup") {
        opts.optimize_for_point_lookup(o.as_u64().unwrap_or(1024));
    }

    if let Some(o) = options.get("set_table_cache_num_shard_bits") {
        opts.set_table_cache_num_shard_bits(o.as_u64().unwrap_or(6) as i32);
    }

    if let Some(o) = options.get("set_max_write_buffer_number") {
        opts.set_max_write_buffer_number(o.as_u64().unwrap_or(2) as i32);
    }

    if let Some(o) = options.get("set_write_buffer_size") {
        opts.set_write_buffer_size(o.as_u64().unwrap_or(64 * 1024 * 1024) as usize);
    }

    if let Some(o) = options.get("set_target_file_size_base") {
        opts.set_target_file_size_base(o.as_u64().unwrap_or(64 * 1024 * 1024));
    }

    if let Some(o) = options.get("set_min_write_buffer_number_to_merge") {
        opts.set_min_write_buffer_number_to_merge(o.as_u64().unwrap_or(1) as i32);
    }

    if let Some(o) = options.get("set_level_zero_stop_writes_trigger") {
        opts.set_level_zero_stop_writes_trigger(o.as_u64().unwrap_or(24) as i32);
    }

    if let Some(o) = options.get("set_level_zero_slowdown_writes_trigger") {
        opts.set_level_zero_slowdown_writes_trigger(o.as_u64().unwrap_or(20) as i32);
    }

    if let Some(o) = options.get("set_compaction_style") {
        let style = o.as_str().unwrap_or("level");

        if style == "universal" {
            opts.set_compaction_style(rocksdb::DBCompactionStyle::Universal);
        }

        if style == "level" {
            opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        }

        if style == "fifo" {
            opts.set_compaction_style(rocksdb::DBCompactionStyle::Fifo);
        }
    }

    if let Some(o) = options.get("set_disable_auto_compactions") {
        opts.set_disable_auto_compactions(o.as_bool().unwrap_or(false));
    }

    opts
}
