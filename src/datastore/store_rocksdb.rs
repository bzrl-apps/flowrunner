use std::time::Duration;
use rocksdb::{DB, Options, ColumnFamilyDescriptor};
use std::{sync::Arc, collections::HashMap};

use serde_json::{Map, Value};

use anyhow::{Result, anyhow};

use log::debug;

use crate::datastore::store::{Store, StoreConfig};

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
    db_opts: Options,
    //ns_opts: HashMap<String, Options>
}

impl RocksDB {
    pub fn init(config: &StoreConfig) -> Self {
        let mut cfs: Vec<ColumnFamilyDescriptor> = Vec::new();
        let mut ns_opts: HashMap<String, Options> = HashMap::new();

        for ns in config.namespaces.iter() {
            let cf_opts = set_opts(ns.options.clone());
            cfs.push(ColumnFamilyDescriptor::new(ns.name.clone(), cf_opts.clone()));
            ns_opts.insert(ns.name.clone(), cf_opts.clone());
        }

        let db_opts = set_opts(config.options.clone());

        let db = match config.ttl {
            s if s > 0 => DB::open_cf_descriptors_with_ttl(&db_opts, &config.conn_str, cfs, Duration::from_secs(s)).unwrap(),
            _  => DB::open_cf_descriptors(&db_opts, &config.conn_str, cfs).unwrap(),
        };

        RocksDB {
            db: Arc::new(db),
            db_opts,
            //ns_opts,
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

            return Ok(());
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
                    debug!("Finding '{}' returns None", k);

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

            return Ok(());
        }

        Err(anyhow!("{}", format!("{} not found", ns)))
    }
}

fn set_opts(options: Map<String, Value>) -> Options {
    let mut opts = Options::default();

    if let Some(o) = options.get("create_if_missing") {
        opts.create_if_missing(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("create_missing_column_families") {
        opts.create_missing_column_families(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("set_advise_random_on_open") {
        opts.set_advise_random_on_open(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("set_allow_concurrent_memtable_write") {
        opts.set_allow_concurrent_memtable_write(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("set_allow_mmap_reads") {
        opts.set_allow_mmap_reads(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("set_allow_mmap_writes") {
        opts.set_allow_mmap_writes(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("set_atomic_flush") {
        opts.set_atomic_flush(o.as_bool().unwrap_or(false));
    }

    if let Some(o) = options.get("increase_parallelisms") {
        opts.increase_parallelism(o.as_i64().unwrap_or(1) as i32);
    }

    if let Some(o) = options.get("set_bloom_locality") {
        opts.set_bloom_locality(o.as_i64().unwrap_or(0) as u32);
    }

    if let Some(o) = options.get("set_max_open_files") {
        opts.set_max_open_files(o.as_i64().unwrap_or(-1) as i32);
    }

    if let Some(o) = options.get("set_zstd_max_train_bytes") {
        opts.set_zstd_max_train_bytes(o.as_i64().unwrap_or(0) as i32);
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

    if let Some(o) = options.get("set_arena_block_size") {
        opts.set_arena_block_size(o.as_u64().unwrap_or(0) as usize);
    }

    if let Some(o) = options.get("set_compaction_readahead_size") {
        opts.set_compaction_readahead_size(o.as_u64().unwrap_or(0) as usize);
    }

    if let Some(o) = options.get("set_max_log_file_size") {
        opts.set_max_log_file_size(o.as_u64().unwrap_or(0) as usize);
    }

    if let Some(o) = options.get("set_log_file_time_to_roll") {
        opts.set_log_file_time_to_roll(o.as_u64().unwrap_or(0) as usize);
    }

    if let Some(o) = options.get("set_recycle_log_file_num") {
        opts.set_recycle_log_file_num(o.as_u64().unwrap_or(0) as usize);
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

    if let Some(o) = options.get("set_bottommost_compression_type") {
        let style = o.as_str().unwrap_or("none");

        if style == "snappy" {
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Snappy);
        }

        if style == "zlib" {
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zlib);
        }

        if style == "bz2" {
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Bz2);
        }

        if style == "lz4" {
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Lz4);
        }

        if style == "lz4hc" {
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Lz4hc);
        }

        if style == "zstd" {
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        }
    }

    if let Some(o) = options.get("set_log_level") {
        let level = o.as_str().unwrap_or("info");

        if level == "debug" {
            opts.set_log_level(rocksdb::LogLevel::Debug);
        }

        if level == "info" {
            opts.set_log_level(rocksdb::LogLevel::Info);
        }

        if level == "warn" {
            opts.set_log_level(rocksdb::LogLevel::Warn);
        }

        if level == "error" {
            opts.set_log_level(rocksdb::LogLevel::Error);
        }

        if level == "fatal" {
            opts.set_log_level(rocksdb::LogLevel::Fatal);
        }

        if level == "header" {
            opts.set_log_level(rocksdb::LogLevel::Header);
        }
    }


    if let Some(o) = options.get("set_disable_auto_compactions") {
        opts.set_disable_auto_compactions(o.as_bool().unwrap_or(false));
    }

    //add_comparator
    //add_merge_operator
    //create_if_missing
    //create_missing_column_families
    //enable_statistics
    //get_statistics
    //optimize_level_style_compaction
    //optimize_universal_style_compaction
    //prepare_for_bulk_load
    //set_access_hint_on_compaction_start
    //set_allow_os_buffer
    //set_block_based_table_factory
    //set_bottommost_compression_options
    //set_bottommost_zstd_max_train_bytes
    //set_compaction_filter
    //set_compaction_filter_factory
    //set_comparator
    //set_compression_options
    //set_compression_per_level
    //set_compression_type
    //set_cuckoo_table_factory
    //set_db_log_dir
    //set_db_paths
    //set_db_write_buffer_size
    //set_delete_obsolete_files_period_micros
    //set_dump_malloc_stats
    //set_enable_pipelined_write
    //set_enable_write_thread_adaptive_yield
    //set_env
    //set_error_if_exists
    //set_fifo_compaction_options
    //set_hard_pending_compaction_bytes_limit
    //set_inplace_update_locks
    //set_inplace_update_support
    //set_is_fd_close_on_exec
    //set_keep_log_file_num
    //set_level_compaction_dynamic_level_bytes
    //set_level_zero_file_num_compaction_trigger
    //set_manifest_preallocation_size
    //set_manual_wal_flush
    //set_max_background_compactions
    //set_max_background_flushes
    //set_max_background_jobs
    //set_max_bytes_for_level_base
    //set_max_bytes_for_level_multiplier
    //set_max_bytes_for_level_multiplier_additional
    //set_max_compaction_bytes
    //set_max_file_opening_threads
    //set_max_manifest_file_size
    //set_max_sequential_skip_in_iterations
    //set_max_subcompactions
    //set_max_successive_merges
    //set_max_total_wal_size
    //set_max_write_buffer_size_to_maintain
    //set_memtable_factory
    //set_memtable_huge_page_size
    //set_memtable_prefix_bloom_ratio
    //set_memtable_whole_key_filtering
    //set_merge_operator
    //set_merge_operator_associative
    //set_min_level_to_compress
    //set_min_write_buffer_number
    //set_num_levels
    //set_optimize_filters_for_hits
    //set_paranoid_checks
    //set_plain_table_factory
    //set_prefix_extractor
    //set_ratelimiter
    //set_report_bg_io_stats
    //set_row_cache
    //set_skip_checking_sst_file_sizes_on_db_open
    //set_skip_stats_update_on_db_open
    //set_soft_pending_compaction_bytes_limit
    //set_stats_dump_period_sec
    //set_stats_persist_period_sec
    //set_target_file_size_multiplier
    //set_universal_compaction_options
    //set_unordered_write
    //set_use_adaptive_mutex
    //set_use_direct_io_for_flush_and_compaction
    //set_use_direct_reads
    //set_wal_bytes_per_sync
    //set_wal_dir
    //set_wal_recovery_mode
    //set_wal_size_limit_mb
    //set_wal_ttl_seconds
    //set_writable_file_max_buffer_size

    opts
}
