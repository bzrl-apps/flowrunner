use std::collections::HashMap;
use log::{info, error};
use std::fs::File;

use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value as yamlValue};
use serde_json::Map;
use serde_json::value::Value as jsonValue;

use anyhow::{anyhow, Result};

use async_channel::*;
use tokio::sync::*;

use crate::{
    job::{Task, Job},
    utils,
    source::Source,
};

use crate::message::Message as FlowMessage;

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    Action,
    Stream,
}

// Implement trait Default for FlowKind
impl Default for Kind {
    fn default() -> Self {
        Kind::Action
    }
}

//use crate::inventory::Inventory;

#[derive(Debug ,Serialize, Deserialize, PartialEq)]
pub struct Flow {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
	pub variables:  Map<String, jsonValue>,

    #[serde(default)]
    pub kind: Kind,

    #[serde(default)]
    pub sources: Vec<Source>,
    #[serde(default)]
	pub jobs: Vec<Job>,
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

impl Default for Flow {
    fn default() -> Self {
        Flow {
            name: "".to_string(),
            variables: Map::new(),
            kind: Kind::Action,
            sources: vec![],
            jobs: vec![],
            remote_plugin_dir: "".to_string(),
            remote_exec_dir: "".to_string(),
            inventory_file: "".to_string(),
            is_on_remote: false,
        }
    }
}

impl Flow {
    pub fn new_from_file(file: &str) -> Result<Flow>{
        let f = File::open(file).unwrap();
        let mapping = serde_yaml::from_reader(f).unwrap();

        parse(mapping)
    }

    pub fn new_from_str(content: &str) -> Result<Flow> {
        let mapping: Mapping = serde_yaml::from_str(content).unwrap();

        parse(mapping)
    }

    pub async fn run(&mut self) -> Result<()> {
        if self.jobs.len() <= 0 {
            return Err(anyhow!("No job specified"));
        }

        match self.kind {
            Kind::Stream => {
                info!("Flow kind: Stream");

                if self.sources.len() <= 0 {
                    return Err(anyhow!("At least one source must be specified when using flow stream"));
                }

                // Create boundeds from sources to jobs according to
                // the number of jobs. Each job will receive messages from all sources
                let jobs = self.jobs.clone();
                for (i, mut job) in jobs.into_iter().enumerate() {
                    let (rx, tx) = bounded::<FlowMessage>(1024);
                    job.tx.push(tx);

                    // Report global flow settings in job context
                    job.context.insert("variables".to_string(), jsonValue::from(self.variables.clone()));

                    self.jobs[i] = job;

                    let srcs = self.sources.clone();
                    for (j, mut src) in srcs.into_iter().enumerate() {
                        src.rx.push(rx.clone());
                        self.sources[j] = src;
                    }
                }

                self.launch_source_threads().await;
                self.launch_job_threads().await;
            },
            _ => {
                info!("Flow kind: Action");
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
        let kind_cloned = self.kind.clone();
        tokio::spawn(async move {
            if let Ok(jobs) = run_all_jobs(kind_cloned, jobs_cloned).await {
                if let Err(_) = tx.send(jobs) {
                    error!("Sending jobs result: receiver dropped");
                }
            }
        });

        match rx.await {
            Ok(v) => self.jobs = v,
            Err(_) => println!("Receving jobs result: sender dropped"),
        }
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

async fn run_all_jobs(kind: Kind, jobs: Vec<Job>) -> Result<Vec<Job>> {
    let mut js = jobs.clone();

    for (i, j) in jobs.iter().enumerate() {
        info!("Executing job {}", j.name);

        exec_job(kind, &mut js, i).await?;
    }

    Ok(js.to_vec())
}

async fn exec_job(kind: Kind, jobs: &mut [Job], idx: usize) -> Result<()> {
    let mut job = jobs[idx].clone();

    if job.hosts == "".to_string() || job.hosts == "localhost".to_string() || job.hosts == "127.0.0.1".to_string() {
        let _ = exec_job_local(kind, &mut job).await?;
        jobs[idx] = job;

        println!("Job: {:?}", jobs[idx]);

        return Ok(());
    }

   let _ = exec_job_remote(&mut job)?;
   jobs[idx] = job;

   Ok(())
}

async fn exec_job_local(kind: Kind, job: &mut Job) -> Result<()> {
    info!("Executing locally the job {}", job.name);

    if kind == Kind::Stream {
        let mut job_cloned = job.clone();
        tokio::spawn(async move {
            match job_cloned.run(None).await {
                Ok(()) => (),
                Err(e) => error!("{}", e.to_string()),
            }
        });
    } else {
        job.run(None).await?;
    }

    Ok(())
}

fn exec_job_remote(job: &mut Job) -> Result<()> {
    info!("Executing remotely the job {}", job.name);

    Ok(())
}

fn parse(mapping: Mapping) -> Result<Flow> {
    let mut flow = Flow::default();

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

    if let Some(variables) = mapping.get(&yamlValue::String("variables".to_string())) {
        if let Some(vars) = variables.as_mapping() {
            for (k, v) in vars.iter() {
                let key = k.as_str().ok_or(anyhow!("Error parsing variables' key"))?;
                let val = utils::convert_value_yaml_to_json(v)?;
                flow.variables.insert(key.to_string(), val);
            }
        }
    }

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

                        if s.plugin == "" {
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

                        src_count = src_count + 1;
                    }

                    sources
                },
                None => Vec::new(),
            }
        },
        None => Vec::new(),
    };

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

                                        // Check plugin
                                        if let Some((k, v)) = v2.iter().nth(0) {
                                            t.plugin = k.as_str().unwrap_or("").to_string();
                                            if t.plugin == "" {
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

                                        // Check on_success
                                        if let Some(v) = v2.get(&yamlValue::String("on_success".to_string())) {
                                            t.on_success = v.as_str().unwrap_or("").to_string();
                                        }

                                        // Check on_failure
                                        if let Some(v) = v2.get(&yamlValue::String("on_failure".to_string())) {
                                            t.on_failure = v.as_str().unwrap_or("").to_string();
                                        }

                                        // Set on_success for the previous task to this one
                                        if task_count > 1 && tasks[task_count-2].on_success == "" {
                                            tasks[task_count-2].on_success = t.name.to_owned();
                                        }
                                    }

                                    tasks.push(t);

                                    task_count = task_count + 1;
                                }
                            }
                        }

                        j.tasks = tasks;

                        jobs.push(j);

                        job_count = job_count + 1;
                    }

                    jobs
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
    use crate::job::Status;
    use crate::plugin::PluginRegistry;
    use tokio::time::{sleep, Duration};
    use serde_json::value::Number;
    use rdkafka::{
        config::ClientConfig,
        //message::{Headers, Message},
        producer::future_producer::{FutureProducer, FutureRecord},
    };

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

sources:
  - name: kafka1
    plugin: kafka
    params:
      brokers:
      - 127.0.0.1:1234
      consumer:
        group_id: group1
        topics:
        - name: topic1
          event: event1
        - name: topic2
          event: event2
        offset: earliest
  - plugin: kafka
    params:
      brokers:
      - 127.0.0.1:1234
      consumer:
        group_id: group2
        topics:
        - name: topic2
          event: event2
        offset: earliest
jobs:
  - hosts: host1
    tasks:
    - shell:
        params:
          cmd: "echo task1"
      name: default

  - name: job2
    tasks:
    - shell:
        params:
          cmd: "echo task1"
      on_failure: "task-3"
    - shell:
        params:
          cmd: "echo task2"
      on_success: "task-4"
      on_failure: "task-4"
    - shell:
        params:
          cmd: "echo task3"
    - shell:
        params:
          cmd: "echo task4"

  - name: job3
    hosts: host3
    tasks:
    - shell:
        params:
          cmd: "echo task1"
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

        let mut params_src1_topic1 = Map::new();
        params_src1_topic1.insert("name".to_string(), jsonValue::String("topic1".to_string()));
        params_src1_topic1.insert("event".to_string(), jsonValue::String("event1".to_string()));

        let mut params_src1_topic2 = Map::new();
        params_src1_topic2.insert("name".to_string(), jsonValue::String("topic2".to_string()));
        params_src1_topic2.insert("event".to_string(), jsonValue::String("event2".to_string()));

        let mut params_src1_consumer = Map::new();
        params_src1_consumer.insert("group_id".to_string(), jsonValue::String("group1".to_string()));
        params_src1_consumer.insert("topics".to_string(), jsonValue::Array(vec![jsonValue::Object(params_src1_topic1), jsonValue::Object(params_src1_topic2)]));
        params_src1_consumer.insert("offset".to_string(), jsonValue::String("earliest".to_string()));
        params_src1.insert("consumer".to_string(), jsonValue::Object(params_src1_consumer));

        let mut params_src2 = Map::new();
        params_src2.insert("brokers".to_string(), jsonValue::Array(vec![jsonValue::String("127.0.0.1:1234".to_string())]));

        let mut params_src2_topic2 = Map::new();
        params_src2_topic2.insert("name".to_string(), jsonValue::String("topic2".to_string()));
        params_src2_topic2.insert("event".to_string(), jsonValue::String("event2".to_string()));

        let mut params_src2_consumer = Map::new();
        params_src2_consumer.insert("group_id".to_string(), jsonValue::String("group2".to_string()));
        params_src2_consumer.insert("topics".to_string(), jsonValue::Array(vec![jsonValue::Object(params_src2_topic2)]));
        params_src2_consumer.insert("offset".to_string(), jsonValue::String("earliest".to_string()));
        params_src2.insert("consumer".to_string(), jsonValue::Object(params_src2_consumer));

        let mut sources = Vec::new();
        sources.push(Source {
            name: "kafka1".to_string(),
            plugin: "kafka".to_string(),
            params: params_src1,
            rx: vec![],
            tx: vec![],
        });

        sources.push(Source {
            name: "src-2".to_string(),
            plugin: "kafka".to_string(),
            params: params_src2,
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
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        });

        let mut job2_tasks = Vec::new();
        job2_tasks.push(Task {
            name: "task-1".to_string(),
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        });

        job2_tasks.push(Task {
            name: "task-2".to_string(),
            plugin: "shell".to_string(),
            params: params_task2.clone(),
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        });

        job2_tasks.push(Task {
            name: "task-3".to_string(),
            plugin: "shell".to_string(),
            params: params_task3.clone(),
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        });

        job2_tasks.push(Task {
            name: "task-4".to_string(),
            plugin: "shell".to_string(),
            params: params_task4.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        });

        let mut job3_tasks = Vec::new();
        job3_tasks.push(Task {
            name: "task-1".to_string(),
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        });

        let mut jobs = Vec::new();
        jobs.push(Job {
            name: "job-1".to_string(),
            hosts: "host1".to_string(),
            start: None,
            tasks: job1_tasks,
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
            rx: vec![],
            tx: vec![],
        });

        jobs.push(Job {
            name: "job2".to_string(),
            hosts: "localhost".to_string(),
            start: None,
            tasks: job2_tasks,
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
            rx: vec![],
            tx: vec![],
        });

        jobs.push(Job {
            name: "job3".to_string(),
            hosts: "host3".to_string(),
            start: None,
            tasks: job3_tasks,
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
            rx: vec![],
            tx: vec![],
        });

        let expected = Flow {
            name: "flow1".to_string(),
            variables,
            kind: Kind::Action,
            sources,
            jobs,
            remote_plugin_dir: "".to_string(),
            remote_exec_dir: "".to_string(),
            inventory_file: "".to_string(),
            is_on_remote: false,
            result: HashMap::new(),
        };

        assert_eq!(flow.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_run_all_jobs() {
        env_logger::init();

        let content = r#"
name: flow1

jobs:
  - hosts: host1
    tasks:
    - shell:
        params:
          cmd: "echo task1"
      name: default

  - name: job2
    tasks:
    - shell:
        params:
          cmd: "echo task1"
      on_failure: "task-3"
    - shell:
        params:
          cmd: "echo task2"
      on_success: "task-4"
      on_failure: "task-4"
    - shell:
        params:
          cmd: "echo task3"
    - shell:
        params:
          cmd: "echo task4"

  - name: job3
    hosts: host3
    tasks:
    - shell:
        params:
          cmd: "echo task1"
"#;

        let flow =  Flow::new_from_str(content).unwrap();

        run_all_jobs(flow.kind.clone(), flow.jobs.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn test_flow_srcs_jobs() {
        env_logger::init();

        let content = r#"
name: flow1

kind: stream
sources:
  - name: kafka1
    plugin: builtin_kafka_consumer
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
    - builtin_shell:
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
            Ok(delivery) => println!("Sent delivery status: {:?}", delivery),
            Err((e, _)) => println!("Sent eror: {:?}", e),
        }


        tokio::spawn(async move {
            flow.run().await.unwrap();
        });

        sleep(Duration::from_millis(10000)).await;
    }
}
