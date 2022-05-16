use log::*;
use std::fs::File;

use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value as yamlValue};
use serde_json::Map;
use serde_json::Value as jsonValue;

use anyhow::{anyhow, Result};

use async_channel::*;
use tokio::sync::*;
use tokio::time::Duration;

use crate::datastore::store::StoreNamespace;
use crate::{
    job::{Task, Job},
    utils,
    source::Source,
    sink::Sink,
};

use moka::future::Cache;
use std::sync::{Arc, Mutex};

use crate::message::Message as FlowMessage;
use crate::datastore::store::StoreConfig;

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    Action,
    Stream,
    Cron,
}

// Implement trait Default for FlowKind
impl Default for Kind {
    fn default() -> Self {
        Kind::Action
    }
}

//use crate::inventory::Inventory;

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
pub struct Flow {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub variables:  Map<String, jsonValue>,
    #[serde(default)]
    pub user_payload:  jsonValue,

    #[serde(default)]
    pub kind: Kind,
    #[serde(default)]
    pub schedule: String,

    #[serde(default)]
    pub datastore: Option<StoreConfig>,

    #[serde(default = "default_parallel")]
    job_parallel: bool,

    #[serde(default)]
    pub sources: Vec<Source>,
    #[serde(default)]
    pub jobs: Vec<Job>,
    #[serde(default)]
    pub sinks: Vec<Sink>,
    //#[serde(default)]
	//pub inventory: Inventory,

    #[serde(default)]
    pub remote_plugin_dir: String,
    #[serde(default)]
    pub remote_exec_dir: String,
    #[serde(default)]
    pub inventory_file: String,

	// IsOnRemote indicates if the flow file is on remote machine
	// even if it is local
    #[serde(default)]
	pub is_on_remote: bool,
}

fn default_parallel() -> bool {
    true
}

//impl Default for Flow {
    //fn default() -> Self {
        //Flow {
            //name: "".to_string(),
            //variables: Map::new(),
            //kind: Kind::Action,
            //datastore: Map::new(),
            //sources: vec![],
            //jobs: vec![],
            //sinks: vec![],
            //remote_plugin_dir: "".to_string(),
            //remote_exec_dir: "".to_string(),
            //inventory_file: "".to_string(),
            //is_on_remote: false,
        //}
    //}
//}

impl Flow {
    pub fn new_from_file(file: &str) -> Result<Flow>{
        let f = File::open(file).unwrap();
        let mapping = serde_yaml::from_reader(f).unwrap();

        parse(mapping)
    }

    #[allow(dead_code)]
    pub fn new_from_str(content: &str) -> Result<Flow> {
        let mapping: Mapping = serde_yaml::from_str(content).unwrap();

        parse(mapping)
    }

    pub async fn run(&mut self) -> Result<()> {
        if self.jobs.is_empty() {
            return Err(anyhow!("No job specified"));
        }

        match self.kind {
            Kind::Stream => {
                info!("Flow kind: Stream");

                if self.sources.is_empty() {
                    return Err(anyhow!("At least one source must be specified when using flow stream"));
                }

                // Init a cache for job results in sequential mode
                let cache = if !self.job_parallel {
                    Some(Cache::<String, Arc<Mutex<Map<String, jsonValue>>>>::builder()
                         .max_capacity(1024)
                         .time_to_live(Duration::from_secs(10 * 60))
                         .build())
                } else {
                    None
                };

                // Create boundeds from sources to jobs according to
                // the number of jobs. Each job will receive messages from all sources
                let jobs = self.jobs.clone();
                for (i, mut job) in jobs.into_iter().enumerate() {
                    // Set job cache
                    job.cache = cache.clone();

                    let (rx_src_job, tx_src_job) = bounded::<FlowMessage>(1024);
                    job.tx.push(tx_src_job);

                    // Report global flow settings in context
                    job.context.insert("variables".to_string(), jsonValue::from(self.variables.clone()));
                    job.context.insert("user_payload".to_string(), self.user_payload.clone());

                    self.jobs[i] = job;

                    let srcs = self.sources.clone();
                    for (j, mut src) in srcs.into_iter().enumerate() {
                        // Report global flow settings in context
                        src.context.insert("variables".to_string(), jsonValue::from(self.variables.clone()));
                        src.context.insert("user_payload".to_string(), self.user_payload.clone());
                        src.rx.push(rx_src_job.clone());
                        self.sources[j] = src;
                    }
                }

                // Prepare messaging channels between jobs & sinks
                let sinks = self.sinks.clone();
                for (i, mut sink) in sinks.into_iter().enumerate() {
                    let (rx_job_sink, tx_job_sink) = bounded::<FlowMessage>(1024);
                    sink.tx.push(tx_job_sink);

                    // Report global flow settings in context
                    sink.context.insert("variables".to_string(), jsonValue::from(self.variables.clone()));
                    sink.context.insert("user_payload".to_string(), self.user_payload.clone());

                    self.sinks[i] = sink;

                    let jobs = self.jobs.clone();
                    for (j, mut job) in jobs.into_iter().enumerate() {
                        job.rx.push(rx_job_sink.clone());
                        self.jobs[j] = job;
                    }
                }

                // Prepare messaging channels between jobs, sources & sinks
                // Because sometime a source also can be a sink and receives messages from jobs
                // such as http-server, we need set the same job's receiver for source and sink.
                let srcs = self.sources.clone();
                for (i, mut src) in srcs.into_iter().enumerate() {
                    if src.params.get("is_also_sink")
                        .and_then(|v| v.as_bool())
                        .unwrap_or_default() {

                        let (rx_job_src, tx_job_src) = bounded::<FlowMessage>(1024);
                        src.tx.push(tx_job_src);

                        self.sources[i] = src;

                        let jobs = self.jobs.clone();
                        for (j, mut job) in jobs.into_iter().enumerate() {
                            job.rx.push(rx_job_src.clone());
                            self.jobs[j] = job;
                        }
                    }
                }

                self.launch_source_threads().await;
                self.launch_job_threads().await;
                self.launch_sink_threads().await;
            },
            _ => { // Kind: Cron or Action but shares the same job configuration
                if self.kind == Kind::Cron {
                    info!("Flow kind: Cron");

                    if self.schedule.is_empty() {
                        return Err(anyhow!("Schedule must be specified for flow type Cron"));
                    }
                } else {
                    info!("Flow kind: Action");
                }

                let jobs = self.jobs.clone();
                for (i, mut job) in jobs.into_iter().enumerate() {
                    // Report global flow settings in job context
                    job.context.insert("variables".to_string(), jsonValue::from(self.variables.clone()));
                    job.context.insert("user_payload".to_string(), self.user_payload.clone());

                    self.jobs[i] = job;
                }

                self.launch_job_threads().await;
            },
        }

        Ok(())
    }

    async fn launch_source_threads(&self) {
        // Launch source threads
        let srcs_cloned = self.sources.clone();
        tokio::spawn(async move {
            if let Err(e) = run_all_sources(srcs_cloned).await {
                error!("{}", e.to_string());
            }
        });
    }

    async fn launch_job_threads(&mut self) {
        // Launch job threads
        let (tx, rx) = oneshot::channel();

        let jobs_cloned = self.jobs.clone();
        let datastore_cloned = self.datastore.clone();
        tokio::spawn(async move {
            info!("Run all jobs...");
            match run_all_jobs(jobs_cloned, datastore_cloned).await {
                Ok(jobs) => {
                    if tx.send(jobs).is_err() {
                        error!("Sending jobs result: receiver dropped");
                    }
                },
                Err(e) => error!("{e}"),
            }
        });

        match rx.await {
            Ok(v) => self.jobs = v,
            Err(e) => error!("Receving jobs result: sender dropped: {}", e),
        }
    }

    async fn launch_sink_threads(&self) {
        // Launch sink threads
        let sinks_cloned = self.sinks.clone();
        tokio::spawn(async move {
            if let Err(e) = run_all_sinks(sinks_cloned).await {
                error!("{}", e.to_string());
            }
        });
    }
}

async fn run_all_sources(sources: Vec<Source>) -> Result<()> {
    for s in sources.iter() {
        info!("Executing source {}, nb of rx {}", s.name, s.rx.len());

        let mut src_cloned = s.clone();
        tokio::spawn(async move {
            match src_cloned.run().await {
                Ok(()) => (),
                Err(e) => error!("{}", e.to_string()),
            }
        });
    }

    Ok(())
}

async fn run_all_sinks(sinks: Vec<Sink>) -> Result<()> {
    for s in sinks.iter() {
        info!("Executing sink {}, nb of rx {}", s.name, s.rx.len());

        let mut sink_cloned = s.clone();
        tokio::spawn(async move {
            match sink_cloned.run().await {
                Ok(()) => (),
                Err(e) => error!("{}", e.to_string()),
            }
        });
    }

    Ok(())
}

async fn run_all_jobs(
    jobs: Vec<Job>,
    datastore: Option<StoreConfig>
) -> Result<Vec<Job>> {
    let mut js = jobs.clone();

    for (i, j) in jobs.iter().enumerate() {
        info!("Executing job {}", j.name);

        exec_job(&mut js, i, datastore.clone()).await?;
    }

    Ok(js.to_vec())
}

async fn exec_job(
    jobs: &mut [Job],
    idx: usize,
    datastore: Option<StoreConfig>
) -> Result<()> {
    let mut job = jobs[idx].clone();

    if job.hosts.is_empty() || job.hosts == "localhost" || job.hosts == "127.0.0.1" {
        if let Err(e) = exec_job_local(&mut job, datastore.clone()).await {
            error!("exec_job_local: {e}");
        }

        jobs[idx] = job.clone();

        debug!("Job: {:?}", jobs[idx]);

        return Ok(());
    }

    if let Err(e) = exec_job_remote(&mut job) {
        error!("exec_job_remote: {e}");
    }

    jobs[idx] = job;

    Ok(())
}

async fn exec_job_local(
    job: &mut Job,
    datastore: Option<StoreConfig>
) -> Result<()> {
    info!("Executing locally the job {}", job.name);

    let mut job_cloned = job.clone();
    let datastore_cloned = datastore.clone();
    tokio::spawn(async move {
        match job_cloned.run(None, datastore_cloned).await {
            Ok(()) => (),
            Err(e) => error!("{}", e.to_string()),
        }
    });

    Ok(())
}

fn exec_job_remote(job: &mut Job) -> Result<()> {
    info!("Executing remotely the job {}", job.name);

    Ok(())
}

fn parse(mapping: Mapping) -> Result<Flow> {
    let mut flow = Flow::default();
    flow.job_parallel = true;

    if let Some(s) = mapping.get(&yamlValue::String("name".to_string())) {
        if let Some(v) = s.as_str() {
            flow.name = v.to_string();
        }
    } else {
        return Err(anyhow!("Flow name must be specified!"))
    }

    if let Some(s) = mapping.get(&yamlValue::String("kind".to_string())) {
        if let Some(v) = s.as_str() {
            if v == "stream" {
                flow.kind = Kind::Stream;
            }
        }
    }

    if let Some(k) = mapping.get(&yamlValue::String("kind".to_string()))
        .and_then(|s| s.as_str())
        .and_then(|v| {
            match v {
                "stream" => Some(Kind::Stream),
                "cron" => Some(Kind::Cron),
                _ => Some(Kind::Action),
            }
        }) {
        flow.kind = k;
    }

    if let Some(s) = mapping.get(&yamlValue::String("schedule".to_string()))
        .and_then(|s| s.as_str()) {
        flow.schedule = s.to_string();
    }

    if let Some(variables) = mapping.get(&yamlValue::String("variables".to_string())) {
        if let Some(vars) = variables.as_mapping() {
            for (k, v) in vars.iter() {
                let key = k.as_str().ok_or_else(|| anyhow!("Error parsing variables' key"))?;
                let val = utils::convert_value_yaml_to_json(v)?;
                flow.variables.insert(key.to_string(), val);
            }
        }
    }

    // Parsing Datastore
    if let Some(ds) = mapping.get(&yamlValue::String("datastore".to_string())) {
        if let Some(m) = ds.as_mapping() {
            let mut sc = StoreConfig::default();

            // Check kind
            if let Some(v) = m.get(&yamlValue::String("kind".to_string())) {
                sc.kind = v.as_str().unwrap_or("").to_string();
            }

            if sc.kind.is_empty() {
                return Err(anyhow!("datastore.kind must not be empty!"));
            }

            // Check conn_str
            if let Some(v) = m.get(&yamlValue::String("conn_str".to_string())) {
                sc.conn_str = v.as_str().unwrap_or("").to_string();
            }

            if sc.conn_str.is_empty() {
                return Err(anyhow!("datastore.conn_str must not be empty!"));
            }

            // Check options
            if let Some(v1) = m.get(&yamlValue::String("options".to_string())) {
                let mut options = Map::new();

                if let Some(v2) = v1.as_mapping() {
                    for (k, v) in v2.iter() {
                        options.insert(k.as_str().unwrap_or("").to_string(), utils::convert_value_yaml_to_json(v)?);
                    }
                }

                sc.options = options;
            }

            // Check TTL
            if let Some(v1) = m.get(&yamlValue::String("ttl".to_string())) {
                sc.ttl = v1.as_u64().unwrap_or(0);
            }

            // Check Namespaces
            if let Some(v1) = m.get(&yamlValue::String("namespaces".to_string())) {
                if let Some(seq) = v1.as_sequence() {
                    let mut namespaces: Vec<StoreNamespace> = Vec::new();

                    for s in seq.iter() {
                        let mut ns = StoreNamespace::default();
                        // Check name
                        if let Some(v) = s.get(&yamlValue::String("name".to_string())) {
                            ns.name = v.as_str().unwrap_or("").to_string();
                        }

                        if ns.name.is_empty() {
                            return Err(anyhow!("datastore namespace's name must not be empty!"));
                        }

                        // Check prefix_len
                        if let Some(v) = s.get(&yamlValue::String("prefix_len".to_string())) {
                            ns.prefix_len = Some(v.as_u64().unwrap_or(0) as usize);
                        }

                        // Check options
                        if let Some(v1) = s.get(&yamlValue::String("options".to_string())) {
                            let mut options = Map::new();

                            if let Some(v2) = v1.as_mapping() {
                                for (k, v) in v2.iter() {
                                    options.insert(k.as_str().unwrap_or("").to_string(), utils::convert_value_yaml_to_json(v)?);
                                }
                            }

                            ns.options = options;
                        }

                        namespaces.push(ns);
                    }

                    sc.namespaces = namespaces;
                }
            }

            if sc.namespaces.is_empty() {
                return Err(anyhow!("datastore.namespaces must not be empty"));
            }

            flow.datastore = Some(sc);
        }
    }

    // Parse SOURCES
    flow.sources = match mapping.get(&yamlValue::String("sources".to_string())) {
        Some(srcs) => {
            match srcs.as_sequence() {
                Some(seq) => {
                    let mut sources: Vec<Source> = Vec::new();
                    let mut src_count = 1;

                    for src in seq.iter() {
                        let mut s = Source::default();

                        let default_srcname = "src-".to_owned() + &src_count.to_string();

                        // Check name
                        if let Some(v) = src.get(yamlValue::String("name".to_string())) {
                            s.name = v.as_str().unwrap_or(&default_srcname).to_string();
                        } else {
                            s.name = default_srcname.clone();
                        }

                        // Check plugin
                        if let Some(v) = src.get(yamlValue::String("plugin".to_string())) {
                            s.plugin = v.as_str().unwrap_or("").to_string();
                        }

                        if s.plugin.is_empty() {
                            return Err(anyhow!("Plugin name can not be empty!"));
                        }

                        if let Some(v1) = src.get(yamlValue::String("params".to_string())) {
                            let mut params = Map::new();

                            if let Some(v2) = v1.as_mapping() {
                                for (k, v) in v2.iter() {
                                    params.insert(k.as_str().unwrap_or("").to_string(), utils::convert_value_yaml_to_json(v)?);
                                }
                            }

                            s.params = params;
                        }

                        sources.push(s);

                        src_count += src_count;
                    }

                    sources
                },
                None => Vec::new(),
            }
        },
        None => Vec::new(),
    };

    // Parse JOBS
    flow.jobs = match mapping.get(&yamlValue::String("jobs".to_string())) {
        Some(js) => {
            match js.as_sequence() {
                Some(seq) => {
                    let mut jobs: Vec<Job> = Vec::new();
                    let mut job_count = 1;

                    for job in seq.iter() {
                        let mut j = Job::default();

                        let default_jobname = "job-".to_owned() + &job_count.to_string();

                        if let Some(v) = job.get(yamlValue::String("name".to_string())) {
                            j.name = v.as_str().unwrap_or(&default_jobname).to_string();
                        } else {
                            j.name = default_jobname.clone();
                        }

                        if let Some(v) = job.get(yamlValue::String("hosts".to_string())) {
                            j.hosts = v.as_str().unwrap_or("localhost").to_string();
                        } else {
                            j.hosts = "localhost".to_string();
                        }

                        // Check if condition
                        let yaml_value_if = yamlValue::String("if".to_string());
                        if let Some(v) = job.get(yaml_value_if) {
                            j.r#if = v.as_str().map(|s| s.to_string());
                        }

                        // Check dependent jobs
                        if let Some(v) = job.get(yamlValue::String("depends_on".to_string())) {
                            j.depends_on = v.as_sequence()
                                .and_then(|s| {
                                    Some(s.iter().map(|v| v.as_str().unwrap_or_default().to_string()).collect())
                                })
                                .unwrap_or_default();
                        }

                        if !j.depends_on.is_empty() {
                            warn!("{} has dependent jobs: {:?}. Sequential mode is enabled", j.name, j.depends_on);
                            flow.job_parallel = false;
                        }

                        // Check wait_timeout
                        j.wait_timeout = job.get(yamlValue::String("wait_timeout".to_string()))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(300000);

                        // Check wait_timeout
                        j.wait_interval = job.get(yamlValue::String("wait_interval".to_string()))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(3000);

                        let mut tasks: Vec<Task> = Vec::new();

                        if let Some(v) = job.get(yamlValue::String("tasks".to_string())) {
                            if let Some (v1) = v.as_sequence() {
                                let mut task_count = 1;

                                for i in v1.iter() {
                                    let mut t = Task::default();
                                    let mut task = i.clone();

                                    let default_taskname = "task-".to_owned() + &task_count.to_string();

                                    if let Some(v2) = task.as_mapping_mut() {
                                        // Check task name
                                        if let Some(n) = v2.get(&yamlValue::String("name".to_string())) {
                                            t.name = n.as_str().unwrap_or(&default_taskname).to_string();
                                            v2.remove(&yamlValue::String("name".to_string()));
                                        } else {
                                            t.name = default_taskname.clone();
                                        }

                                        // Check if condition
                                        let yaml_value_if = yamlValue::String("if".to_string());
                                        if let Some(n) = v2.get(&yaml_value_if) {
                                            t.r#if = n.as_str().map(|s| s.to_string());

                                            v2.remove(&yaml_value_if);
                                        }

                                        // Check loop
                                        let yaml_value_loop = yamlValue::String("loop".to_string());
                                        if let Some(n) = v2.get(&yaml_value_loop) {
                                            t.r#loop = match n.as_sequence() {
                                                Some(seq) => {
                                                    let mut items: Vec<jsonValue> = Vec::new();

                                                    for s in seq.iter() {
                                                        items.push(utils::convert_value_yaml_to_json(s)?);
                                                    }

                                                    Some(jsonValue::Array(items))
                                                },
                                                None => n.as_str().map(|s| jsonValue::String(s.to_string())),
                                            };

                                            v2.remove(&yaml_value_loop);
                                        }

                                        // Check loop_tempo
                                        let yaml_value_loop_tempo = yamlValue::String("loop_tempo".to_string());
                                        t.loop_tempo = v2.get(&yaml_value_loop_tempo)
                                            .and_then(|v| v.as_u64())
                                            .map(|v| {
                                                v2.remove(&yaml_value_loop_tempo);
                                                v
                                            });

                                        // Check on_success
                                        let yaml_value_on_success = yamlValue::String("on_success".to_string());
                                        if let Some(v) = v2.get(&yaml_value_on_success) {
                                            t.on_success = v.as_str().unwrap_or("").to_string();

                                            v2.remove(&yaml_value_on_success);
                                        }

                                        // Check on_failure
                                        let yaml_value_on_failure = yamlValue::String("on_failure".to_string());
                                        if let Some(v) = v2.get(&yaml_value_on_failure) {
                                            t.on_failure = v.as_str().unwrap_or("").to_string();

                                            v2.remove(&yaml_value_on_failure);
                                        }

                                        // Check plugin
                                        if let Some((k, v)) = v2.iter().next() {
                                            t.plugin = k.as_str().unwrap_or("").to_string();
                                            if t.plugin.is_empty() {
                                                return Err(anyhow!("Plugin name can not be empty!"));
                                            }

                                            if let Some(v3) = v.get (yamlValue::String("params".to_string())) {
                                                let mut params = Map::new();

                                                if let Some(v4) = v3.as_mapping() {
                                                    for (k, v) in v4.iter() {
                                                        params.insert(k.as_str().unwrap_or("").to_string(), utils::convert_value_yaml_to_json(v)?);
                                                    }
                                                }

                                                t.params = params;
                                            } else {
                                                return Err(anyhow!("Plugin params is missing!"));
                                            }
                                        } else {
                                            return Err(anyhow!("Cannot parse the job {}, task {}", j.name, default_taskname));
                                        }

                                        // Set on_success for the previous task to this one
                                        if task_count > 1 && tasks[task_count-2].on_success.is_empty() {
                                            tasks[task_count-2].on_success = t.name.to_owned();
                                        }
                                    }

                                    tasks.push(t);

                                    task_count += 1;
                                }
                            }
                        }

                        j.tasks = tasks;

                        jobs.push(j);

                        job_count += 1;
                    }

                    jobs
                },
                None => Vec::new(),
            }
        },
        None => Vec::new(),
    };

    // Check if dependent jobs specified in each job exist
    let job_names: Vec<String> = flow.jobs.iter().map(|v| v.name.clone()).collect();
    if let Some(j) = flow.jobs.iter()
        .filter(|v| !v.depends_on.is_empty())
        .find(|v| {
            v.depends_on.iter().any(|v| !job_names.contains(&v))
        }) {
        return Err(anyhow!(format!("{} has unknown dependent jobs: depends_on={:?}", j.name, j.depends_on)));
    }

    // Parse SINKS
    flow.sinks = match mapping.get(&yamlValue::String("sinks".to_string())) {
        Some(sks) => {
            match sks.as_sequence() {
                Some(seq) => {
                    let mut sinks: Vec<Sink> = Vec::new();
                    let mut sink_count = 1;

                    for sk in seq.iter() {
                        let mut s = Sink::default();

                        let default_sinkname = "sink-".to_owned() + &sink_count.to_string();

                        // Check name
                        if let Some(v) = sk.get(yamlValue::String("name".to_string())) {
                            s.name = v.as_str().unwrap_or(&default_sinkname).to_string();
                        } else {
                            s.name = default_sinkname.clone();
                        }

                        // Check if condition
                        let yaml_value_if = yamlValue::String("if".to_string());
                        if let Some(v) = sk.get(yaml_value_if) {
                            s.r#if = v.as_str().map(|s| s.to_string());
                        }

                        // Check plugin
                        if let Some(v) = sk.get(yamlValue::String("plugin".to_string())) {
                            s.plugin = v.as_str().unwrap_or("").to_string();
                        }

                        if s.plugin.is_empty() {
                            return Err(anyhow!("Plugin name can not be empty!"));
                        }

                        if let Some(v1) = sk.get(yamlValue::String("params".to_string())) {
                            let mut params = Map::new();

                            if let Some(v2) = v1.as_mapping() {
                                for (k, v) in v2.iter() {
                                    params.insert(k.as_str().unwrap_or("").to_string(), utils::convert_value_yaml_to_json(v)?);
                                }
                            }

                            s.params = params;
                        }

                        sinks.push(s);

                        sink_count += 1;
                    }

                    sinks
                },
                None => Vec::new(),
            }
        },
        None => Vec::new(),
    };

    if let Some(s) = mapping.get(&yamlValue::String("remote_plugin_dir".to_string())) {
        if let Some(v) = s.as_str() {
            flow.remote_plugin_dir = v.to_string();
        }
    }

    if let Some(s) = mapping.get(&yamlValue::String("remote_exec_dir".to_string())) {
        if let Some(v) = s.as_str() {
            flow.remote_exec_dir = v.to_string();
        }
    }

    if let Some(s) = mapping.get(&yamlValue::String("inventory_file".to_string())) {
        if let Some(v) = s.as_str() {
            flow.inventory_file = v.to_string();
        }
    }

    if let Some(s) = mapping.get(&yamlValue::String("is_on_remote".to_string())) {
        if let Some(v) = s.as_bool() {
            flow.is_on_remote = v;
        }
    }

    Ok(flow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::PluginRegistry;
    use ::futures::TryFutureExt;
    use tokio::time::{sleep, Duration};
    use serde_json::{Number, json};
    use rdkafka::{
        config::ClientConfig,
        //message::{Headers, Message},
        producer::future_producer::{FutureProducer, FutureRecord},
    };

    use json_ops::json_map;

    #[test]
    fn test_new_from_str() {
        let content = r#"
name: flow1

variables:
  var1: val1
  var2: true
  var3: 17.05
  var4:
  - val41
  - val42

datastore:
  kind: rocksdb
  conn_str: /tmp/rocksdb
  namespaces:
  - name: ns1
  - name: ns2

sources:
  - name: kafka1
    plugin: kafka
    params:
      brokers:
      - 127.0.0.1:1234
      consumer:
        group_id: group1
        topics:
        - topic1
        - topic2
        offset: earliest
  - plugin: kafka
    params:
      brokers:
      - 127.0.0.1:1234
      consumer:
        group_id: group2
        topics:
        - topic2
        offset: earliest
jobs:
  - hosts: host1
    tasks:
    - builtin-shell:
        params:
          cmd: "echo task1"
      name: default
      loop:
      - "{{ array }}"
      - 1
      - item1

  - name: job2
    if: "source == true"
    tasks:
    - builtin-shell:
        params:
          cmd: "echo task1"
      on_failure: "task-3"
    - builtin-shell:
        params:
          cmd: "echo task2"
      on_success: "task-4"
      on_failure: "task-4"
    - builtin-shell:
        params:
          cmd: "echo task3"
    - builtin-shell:
        params:
          cmd: "echo task4"

  - name: job3
    hosts: host3
    tasks:
    - builtin-shell:
        params:
          cmd: "echo task1"
      loop: "{{ array }}"

sinks:
- name: pg1
  if: "job == job1"
  plugin: builtin-pgql-sqlx
  params:
    conn_str: "postgres://localhost:5432/flowrunner"
    max_conn: 5
    stmts:
    - stmt: "SELECT * FROM users;"
      cond: "true"
      fetch: "one"
"#;

        let flow =  Flow::new_from_str(content);

        let mut variables = Map::new();
        variables.insert("var1".to_string(), jsonValue::String("val1".to_string()));
        variables.insert("var2".to_string(), jsonValue::Bool(true));
        variables.insert("var3".to_string(), jsonValue::Number(Number::from_f64(17.05).unwrap()));
        variables.insert("var4".to_string(), jsonValue::Array(vec![jsonValue::String("val41".to_string()), jsonValue::String("val42".to_string())]));

        // sources
        let mut params_src1 = Map::new();
        params_src1.insert("brokers".to_string(), jsonValue::Array(vec![jsonValue::String("127.0.0.1:1234".to_string())]));

        let mut params_src1_consumer = Map::new();
        params_src1_consumer.insert("group_id".to_string(), jsonValue::String("group1".to_string()));
        params_src1_consumer.insert("topics".to_string(), jsonValue::Array(vec![jsonValue::String("topic1".to_string()), jsonValue::String("topic2".to_string())]));
        params_src1_consumer.insert("offset".to_string(), jsonValue::String("earliest".to_string()));
        params_src1.insert("consumer".to_string(), jsonValue::Object(params_src1_consumer));

        let mut params_src2 = Map::new();
        params_src2.insert("brokers".to_string(), jsonValue::Array(vec![jsonValue::String("127.0.0.1:1234".to_string())]));

        let mut params_src2_consumer = Map::new();
        params_src2_consumer.insert("group_id".to_string(), jsonValue::String("group2".to_string()));
        params_src2_consumer.insert("topics".to_string(), jsonValue::Array(vec![jsonValue::String("topic2".to_string())]));
        params_src2_consumer.insert("offset".to_string(), jsonValue::String("earliest".to_string()));
        params_src2.insert("consumer".to_string(), jsonValue::Object(params_src2_consumer));

        let mut sources = Vec::new();
        sources.push(Source {
            name: "kafka1".to_string(),
            plugin: "kafka".to_string(),
            params: params_src1,
            context: Map::new(),
            rx: vec![],
            tx: vec![],
        });

        sources.push(Source {
            name: "src-2".to_string(),
            plugin: "kafka".to_string(),
            params: params_src2,
            context: Map::new(),
            rx: vec![],
            tx: vec![],
        });

        let params_sink1 = json_map!(
            "conn_str" => jsonValue::String("postgres://localhost:5432/flowrunner".to_string()),
            "max_conn" => jsonValue::Number(Number::from(5)),
            "stmts" => jsonValue::Array(vec![
                jsonValue::Object(json_map!(
                    "stmt" => jsonValue::String("SELECT * FROM users;".to_string()),
                    "cond" => jsonValue::String("true".to_string()),
                    "fetch" => jsonValue::String("one".to_string())
                ))
            ])
        );

        let mut sinks = Vec::new();
        sinks.push(Sink {
            name: "pg1".to_string(),
            r#if: Some("job == job1".to_string()),
            plugin: "builtin-pgql-sqlx".to_string(),
            params: params_sink1,
            context: Map::new(),
            rx: vec![],
            tx: vec![],
        });

        // Task / Job
        let mut params_task1 = Map::new();
        params_task1.insert("cmd".to_string(), jsonValue::String("echo task1".to_string()));

        let mut params_task2 = Map::new();
        params_task2.insert("cmd".to_string(), jsonValue::String("echo task2".to_string()));

        let mut params_task3 = Map::new();
        params_task3.insert("cmd".to_string(), jsonValue::String("echo task3".to_string()));

        let mut params_task4 = Map::new();
        params_task4.insert("cmd".to_string(), jsonValue::String("echo task4".to_string()));

        let mut job1_tasks = Vec::new();
        job1_tasks.push(Task {
            name: "default".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            r#loop: Some(jsonValue::Array(vec![
                jsonValue::String("{{ array }}".to_string()),
                jsonValue::Number(Number::from(1)),
                jsonValue::String("item1".to_string())
            ])),
            on_success: "".to_string(),
            on_failure: "".to_string()
        });

        let mut job2_tasks = Vec::new();
        job2_tasks.push(Task {
            name: "task-1".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            r#loop: None,
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        });

        job2_tasks.push(Task {
            name: "task-2".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task2.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        });

        job2_tasks.push(Task {
            name: "task-3".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task3.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        });

        job2_tasks.push(Task {
            name: "task-4".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task4.clone(),
            r#loop: None,
            on_success: "".to_string(),
            on_failure: "".to_string()
        });

        let mut job3_tasks = Vec::new();
        job3_tasks.push(Task {
            name: "task-1".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            r#loop: Some(jsonValue::String("{{ array }}".to_string())),
            on_success: "".to_string(),
            on_failure: "".to_string()
        });

        let mut jobs = Vec::new();
        jobs.push(Job {
            name: "job-1".to_string(),
            r#if: None,
            hosts: "host1".to_string(),
            start: None,
            tasks: job1_tasks,
            context: Map::new(),
            result: Map::new(),
            rx: vec![],
            tx: vec![],
        });

        jobs.push(Job {
            name: "job2".to_string(),
            r#if: Some("source == true".to_string()),
            hosts: "localhost".to_string(),
            start: None,
            tasks: job2_tasks,
            context: Map::new(),
            result: Map::new(),
            rx: vec![],
            tx: vec![],
        });

        jobs.push(Job {
            name: "job3".to_string(),
            r#if: None,
            hosts: "host3".to_string(),
            start: None,
            tasks: job3_tasks,
            context: Map::new(),
            result: Map::new(),
            rx: vec![],
            tx: vec![],
        });

        let mut datastore = StoreConfig::default();
        datastore.kind = "rocksdb".to_string();
        datastore.conn_str = "/tmp/rocksdb".to_string();
        datastore.namespaces = vec![
            StoreNamespace {name: "ns1".to_string(), prefix_len: None,  options: Map::new()},
            StoreNamespace {name: "ns2".to_string(), prefix_len: None, options: Map::new()},
        ];

        let expected = Flow {
            name: "flow1".to_string(),
            variables,
            user_payload: jsonValue::Null,
            kind: Kind::Action,
            datastore: Some(datastore),
            sources,
            jobs,
            sinks,
            remote_plugin_dir: "".to_string(),
            remote_exec_dir: "".to_string(),
            inventory_file: "".to_string(),
            is_on_remote: false,
        };

        assert_eq!(flow.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_flow_run() {
        let _ =  env_logger::try_init();

        let content = r#"
name: flow1

variables:
    superloop:
    - loop1
    - loop2

kind: action
jobs:
  - hosts: host1
    tasks:
    - builtin-shell:
        params:
          cmd: "echo task1"
      name: default

  - name: job2
    tasks:
    - builtin-shell:
        params:
          cmd: "echo task1"
      on_failure: "task-3"
    - builtin-shell:
        params:
          cmd: "echo task2"
      on_success: "task-4"
      on_failure: "task-4"
    - builtin-shell:
        params:
          cmd: "echo task3"
    - builtin-shell:
        params:
          cmd: "echo task4"

  - name: job3
    tasks:
    - builtin-shell:
        params:
          cmd: "echo {{ loop_item }}"
      loop: "{{ context.variables.superloop | json_encode() | safe }}"
"#;

        let mut flow =  Flow::new_from_str(content).unwrap();
        PluginRegistry::load_plugins("target/debug").await;

        //run_all_jobs(flow.kind.clone(), flow.jobs.clone(), None).await.unwrap();
        flow.run().await.unwrap();

        // Because hosts is not local and we are not supporting remote host
        let result_expected_1 = json!({});

        let result_expected_2 = json!({
            "task-1": {
                "status": "Ok",
                "error": "",
                "output": {
                    "rc": 0,
                    "stdout": "task1\n"
                }
            },
            "task-2": {
                "status": "Ok",
                "error": "",
                "output": {
                    "rc": 0,
                    "stdout": "task2\n"
                }
            },
            "task-4": {
                "status": "Ok",
                "error": "",
                "output": {
                    "rc": 0,
                    "stdout": "task4\n"
                }
            }
        });

        let result_expected_3 = json!({
                "task-1": [
                    {
                        "status": "Ok",
                        "error": "",
                        "output": {
                            "rc": 0,
                            "stdout": "loop1\n"
                        }
                    },
                    {
                        "status": "Ok",
                        "error": "",
                        "output": {
                            "rc": 0,
                            "stdout": "loop2\n"
                        }
                    },
                ]
            });

        assert_eq!(result_expected_1.as_object().unwrap().to_owned(), flow.jobs[0].result);
        assert_eq!(result_expected_2.as_object().unwrap().to_owned(), flow.jobs[1].result);
        assert_eq!(result_expected_3.as_object().unwrap().to_owned(), flow.jobs[2].result);
    }

    #[tokio::test]
    async fn test_flow_srcs_jobs() {
        let _ =  env_logger::try_init();

        let content = r#"
name: flow1

kind: stream
sources:
  - name: kafka1
    plugin: builtin-kafka-consumer
    params:
      brokers:
      - localhost:9092
      consumer:
        group_id: group1
        topics:
        - name: topic1
          event: event1
        offset: earliest

jobs:
  - hosts: localhost
    tasks:
    - builtin-shell:
        params:
          cmd: "echo {{ context.data.message }}"
"#;

        let mut flow =  Flow::new_from_str(content).unwrap();

        PluginRegistry::load_plugins("target/debug").await;

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
            Ok(delivery) => debug!("Sent delivery status: {:?}", delivery),
            Err((e, _)) => error!("Sent eror: {:?}", e),
        }


        tokio::spawn(async move {
            flow.run().await.unwrap();
        });

        sleep(Duration::from_millis(10000)).await;
    }
}
