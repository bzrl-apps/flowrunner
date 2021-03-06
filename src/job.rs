use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value, json};

use anyhow::{Result, anyhow};
use std::collections::HashMap;

use evalexpr::*;

use log::{info, debug, error, warn};

//use tokio::sync::mpsc::*;
use tokio::time::{sleep, Duration, timeout};
use async_channel::*;

use moka::future::Cache;
use std::sync::{Arc, Mutex};

use crate::datastore::store::{BoxStore, StoreConfig};
use crate::plugin::{PluginRegistry, Status as PluginStatus};
use crate::message::Message as FlowMessage;
use crate::utils::*;

#[macro_export]
macro_rules! job_result {
    ($( $key:expr => $val:expr ), *) => {
        {
            let mut result = Map::<String, Value>::new();
            $( result.insert($key.to_string(), $val); )*
            result
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq)]
pub enum Status {
    Ko = 0,
    Ok = 1,
}

// Implement trait Default for Status
impl Default for Status {
    fn default() -> Self {
        Status::Ok
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct Job {
    #[serde(default)]
	pub name: String,
    #[serde(default)]
	pub hosts: String,
	#[serde(default)]
	pub r#if: Option<String>,
    #[serde(default)]
	pub start: Option<Task>,

	#[serde(default)]
	pub status: Status,

    #[serde(default)]
	pub tasks: Vec<Task>,
    #[serde(default)]
	pub context: Map<String, Value>,

    #[serde(default)]
	pub result: Map<String, Value>,

    #[serde(skip_serializing, skip_deserializing)]
	pub rx: Vec<Sender<FlowMessage>>,
    #[serde(skip_serializing, skip_deserializing)]
	pub tx: Vec<Receiver<FlowMessage>>,

    // Options to control sequential execution mode
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default = "default_wait_interval")]
    pub wait_interval: u64,
    #[serde(default = "default_wait_timeout")]
    pub wait_timeout: u64,
    #[serde(skip_serializing, skip_deserializing)]
	pub cache: Option<Cache<String, Arc<Mutex<Map<String, Value>>>>>
}

fn default_wait_interval() -> u64 {
    3000
}

fn default_wait_timeout() -> u64 {
    300000
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("name", &self.name)
            .field("hosts", &self.hosts)
            .field("if", &self.r#if)
            .field("start", &self.start)
            .field("status", &self.status)
            .field("tasks", &self.tasks)
            .field("context", &self.context)
            .field("result", &self.result)
            .field("rx", &self.rx)
            .field("tx", &self.tx)
            //.field("cache", f.debug_map().
            .finish()
    }
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Task {
    #[serde(default)]
	pub name: String,
    #[serde(default)]
    pub r#if: Option<String>,
    #[serde(default)]
    pub plugin: String,
    #[serde(default)]
	pub params: Map<String, Value>,

    #[serde(default)]
	pub r#loop: Option<Value>,
    #[serde(default)]
	pub loop_tempo: Option<u64>,

	#[serde(default)]
	pub register: Map<String, Value>,

    #[serde(default)]
	pub on_success: String,
    #[serde(default)]
	pub on_failure: String
}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name &&
        self.hosts == other.hosts &&
        self.tasks == other.tasks &&
        self.context == other.context &&
        self.result == other.result
    }
}

impl Job {
    pub async fn run(&mut self, tasks: Option<&str>, datastore: Option<StoreConfig>) -> Result<()> {
        info!("JOB RUN STARTED: job={}, hosts={}, nb_rx={}, nb_tx={}", self.name, self.hosts, self.rx.len(), self.tx.len());
        debug!("Job context: {:?}", self.context);

        // Check the name of all tasks indicated in taskflow
        self.check_tasks()?;

        let ts = tasks.unwrap_or("");

        if !self.tx.is_empty() {
            loop {
                // Reinit the current keys added when receiving each message
                debug!("Reinitialize job's status, result & context: job={}, status={:?}", self.name, self.status);
                self.status = Status::default();
                self.result.clear();
                let _ = self.context.remove("msg_id");
                let _ = self.context.remove("register");
                let _ = self.context.remove("user_payload");

                match self.tx[0].recv().await {
                    // Add message received as data in job context
                    Ok(msg) => {
                        let uuid = match msg {
                            FlowMessage::JsonWithSender{ uuid: id, sender: s, source: src, value: v } => {
                                let mut msg_id = self.context
                                    .get("msg_id")
                                    .and_then(|v| v.as_object())
                                    .unwrap_or(&Map::new()).to_owned();
                                msg_id.insert("uuid".to_string(), Value::String(id.clone()));
                                msg_id.insert("sender".to_string(), Value::String(s));
                                msg_id.insert("source".to_string(), Value::String(src.unwrap_or_else(|| "".to_string())));
                                msg_id.insert("data".to_string(), v);

                                self.context.insert("msg_id".to_string(), Value::Object(msg_id));

                                id
                            },
                            _ => {
                                error!("Message received is not Message::Json type: job={}", self.name);
                                continue;
                            },
                       };

                        // Wait for all dependent jobs be executed
                        if let Err(e) = timeout(Duration::from_millis(self.wait_timeout),
                                                self.wait_dependend_jobs(&uuid))
                            .await {
                            error!("Timeout to wait for dependent jobs getting executed: job={}, timeout={}, err={e}",
                                   self.name,
                                   self.wait_timeout);
                            continue;
                        }

                        // Run certain tasks given in parameter
                        let (res, msg_err) = if !ts.is_empty() {
                            (self.run_task_by_task(ts, datastore.clone()).await,
                            "failed to run task by task".to_string())
                        } else {
                            // Run complete taskflow by running the first task
                            (self.run_all_tasks(self.start.clone(), datastore.clone()).await,
                            "failed to run all tasks".to_string())
                        };

                        // Cache the result
                        if let Some(cache) = self.cache.clone() {
                            let uuid_cloned = uuid.clone();
                            let name_cloned = self.name.clone();
                            let status_cloned = self.status;
                            let result_cloned = self.result.clone();
                            let mut job_result = cache.get_with(uuid_cloned, async move {
                                let mut jr = Map::new();
                                jr.insert(name_cloned, json!({
                                    "status": status_cloned,
                                    "result": result_cloned
                                }));
                                Arc::new(Mutex::new(jr))
                            }).await;

                            debug!("Getting current cache value before update: job={}, uuid={}, value={:?}",
                                   self.name, uuid, job_result.lock().unwrap());

                            job_result.lock().unwrap().insert(self.name.clone(), json!({
                                "status": self.status,
                                "result": self.result
                            }));

                            info!("Updating job results cache: job={}, uuid={}", self.name, uuid);
                            cache.insert(uuid.clone(), job_result).await;

                            job_result = cache.get(&uuid).unwrap_or(Arc::new(Mutex::new(Map::new())));
                            debug!("Getting cache value after update: job={}, uuid={}, value={:?}",
                                   self.name, uuid, job_result.lock().unwrap());
                        }

                        // If job run encounter errors, zap to next message
                        if let Err(e) = res {
                            error!("{}: job={}, err={e}", msg_err, self.name);
                            continue;
                        }

                        for rx1 in self.rx.iter() {
                            let msg = FlowMessage::JsonWithSender {
                                uuid: uuid.clone(),
                                sender: self.name.clone(),
                                source: None,
                                value: Value::Object(self.result.clone()),
                            };
                            match rx1.send(msg).await {
                                Ok(()) => (),
                                Err(e) => error!("failed to send job's result: job={}, err={}", self.name, e.to_string()),
                            };
                        }
                    },
                    Err(e) => { error!("{}", e.to_string()); break; },
                }
            }
        } else {
            // Run certain tasks given in parameter
            if !ts.is_empty() {
                self.run_task_by_task(ts, datastore).await?;
            } else {
                // Run complete taskflow by running the first task
                self.run_all_tasks(self.start.clone(), datastore).await?;
            }
        }

        Ok(())
    }

    async fn wait_dependend_jobs(&mut self, uuid: &String) {
        // We need to verify if the current job has any dependant jobs and all of
        // them are already executed.
        if !self.depends_on.is_empty() {
            info!("Waiting for depending jobs getting executed...: job={}, depends_on={:?}",
                    self.name, self.depends_on);

            let mut wait = true;
            while wait {
                wait = self.cache.clone()
                    .and_then(|c| c.get(uuid))
                    .and_then(|v| {
                        debug!("Current cache value: job={}, uuid={}, value={:?}",
                                self.name, uuid, v);

                        let job_results = v.lock().unwrap();

                        let mut unsatisfied = false;
                        for j in self.depends_on.clone() {
                            if !(*job_results).contains_key(&j) {
                                unsatisfied = true
                            }
                        }

                        // When all dependent jobs are executed, we need to update context their
                        // results to be used in the next job's template rendering
                        if !unsatisfied {
                            self.context.insert("job_results".to_string(), Value::Object((*job_results).clone()));
                        }

                        Some(unsatisfied)
                    })
                    .unwrap_or(true);
                info!("Waiting state: job={}, wait={}", self.name, wait);

                if wait {
                    info!("Start waiting for {}ms...: job={}", self.wait_interval, self.name);
                    sleep(Duration::from_millis(self.wait_interval)).await;
                }
            }
        }
    }

    async fn run_all_tasks(&mut self, start: Option<Task>, datastore: Option<StoreConfig>) -> Result<()> {
        let mut next_task: Option<Task> = match start {
            Some(task) => Some(task),
            None => Some(self.tasks[0].clone()),
        };

        // If job condition is not satisfied, then exit
        if !self.render_job_and_eval()? {
            return Ok(());
        }

        // Execute task
        while let Some(mut t) = next_task.to_owned() {
            info!("Task will be executed: name={}, params={:?}, register={:?}",
                  t.name, t.params, t.register);

            let mut task_result = PluginStatus::Ok;

            // If task condition is not satisfied, then move to next task as when the task is
            // correctly executed
            let vec_params = self.render_task_template(&mut t)?;
            if vec_params.is_empty() {
                next_task = match t.on_success.len() {
                    n if n > 0 => self.get_task_by_name(t.on_success.as_str()),
                    _ => None,
                };

                continue;
            }

            debug!("Task's params array: len={}, params={:?}", vec_params.len(), vec_params);

            // Init datastore if configured
            let mut bs: Option<BoxStore> = None;

            if let Some(ds) = datastore.clone() {
                bs = Some(ds.new_store()?);
            }

            match PluginRegistry::get_plugin(&t.plugin) {
                Some(mut plugin) => {
                    let mut vec_res: Vec<Value> = Vec::new();

                    for p in vec_params.iter() {
                        debug!("Treating params array item: p={:?}", p);
                        plugin.validate_params(p.clone())?;
                        plugin.set_datastore(bs.clone());
                        let res = plugin.func(Some(self.name.clone()), &self.rx, &self.tx).await;
                        vec_res.push(serde_json::to_value(res.clone())?);

                        info!("Task result: name {}, res: {:?}",  t.name.clone(), res);

                        if res.status == PluginStatus::Ko {
                            task_result = PluginStatus::Ko;
                        }

                        // Check if loop_tempo is set
                        if let Some(tempo) = t.loop_tempo {
                            debug!("Waiting for the next execution: tempo={}", tempo);
                            std::thread::sleep(Duration::from_millis(tempo));
                        }
                    }

                    if vec_params.len() == 1 {
                        self.result.insert(t.name.clone(), vec_res[0].clone());
                    } else {
                        self.result.insert(t.name.clone(), json!({
                            "status": task_result,
                            "error": "",
                            "output": Value::Array(vec_res)
                        }));
                    }

                    // Render register if not empty
                    if !t.register.is_empty() {
                        self.render_register(&t.register)?;
                    }
                },
                None => error!("No plugin found"),
            };

             // Move to next task on success when PluginStatus::Ok or no plugin found
            if task_result == PluginStatus::Ok {
                next_task = match t.on_success.len() {
                    n if n > 0 => self.get_task_by_name(t.on_success.as_str()),
                    _ => None,
                };
            } else {
                // Go the task of Failure if specified
                next_task = match t.on_failure.len() {
                    n if n == 0 => None,
                    _ => self.get_task_by_name(t.on_failure.as_str()),
                };
                // Update job's status to Ko when a task failed
                self.status = Status::Ko;
            }
        }

        Ok(())
    }

    async fn run_task_by_task(&mut self, tasks: &str, datastore: Option<StoreConfig>) -> Result<()> {
        // If job condition is not satisfied then exit
        if !self.render_job_and_eval()? {
            return Ok(())
        }

        for s in tasks.split(',') {
            match self.get_task_by_name(s) {
                Some(mut t) => {
                    info!("Task executed: name={}, params={:?}, register={:?}",
                          t.name, t.params, t.register);

                    // If task condition is not satisfied then move to next one
                    let vec_params = self.render_task_template(&mut t)?;
                    if vec_params.is_empty() {
                        continue
                    }

                    // Init datastore if configured
                    let mut bs: Option<BoxStore> = None;

                    if let Some(ds) = datastore.clone() {
                        bs = Some(ds.new_store()?);
                    }

                    match PluginRegistry::get_plugin(&t.plugin) {
                        Some(mut plugin) => {
                            let mut vec_res: Vec<Value> = Vec::new();

                            for p in vec_params.iter() {
                                plugin.validate_params(p.clone())?;
                                plugin.set_datastore(bs.clone());
                                let res = plugin.func(Some(self.name.clone()), &self.rx, &self.tx).await;
                                self.result.insert(t.name.clone(), serde_json::to_value(res.clone())?);

                                vec_res.push(serde_json::to_value(res.clone())?);

                                // Update job's status to Ko when a task failed
                                if res.status == PluginStatus::Ko {
                                    self.status = Status::Ko;
                                }

                                info!("Task result: name {}, res: {:?}",  t.name.clone(), res);
                            }

                            if vec_params.len() == 1 {
                                self.result.insert(t.name.clone(), vec_res[0].clone());
                            } else {
                                self.result.insert(t.name.clone(), json!({
                                    "status": self.status,
                                    "error": "",
                                    "output": Value::Array(vec_res)
                                }));
                            }

                            // Render register if not empty
                            if !t.register.is_empty() {
                                self.render_register(&t.register)?;
                            }
                        },
                        None => error!("No plugin with the name {} found", t.name),
                    }
                },
                None => warn!("Task {} not found => ignored!", s),
            }
        }

        Ok(())
    }

    // Checks all tasks to see if the name given for task on
    // failure or on success matches valid task names
    fn check_tasks(&self) -> Result<()> {
        let map: HashMap<_, _> = self.tasks.iter().map(|t| (t.name.clone(), t.clone())).collect();

        for t in self.tasks.iter() {
            if PluginRegistry::get_plugin(&t.plugin).is_none() {
                return Err(anyhow!("{}", format!("Plugin {} is not found", t.plugin)));
            }

            if !t.on_failure.is_empty() && !map.contains_key(&t.on_failure) {
                return Err(anyhow!("task ".to_owned() + &t.on_failure + " is not found"));
            }

            if !t.on_failure.is_empty() && !map.contains_key(&t.on_success) {
                return Err(anyhow!("task ".to_owned() + &t.on_success + " is not found"));
            }
        }

        Ok(())
    }

    fn get_task_by_name(&self, name: &str) -> Option<Task> {
        for t in self.tasks.iter() {
            if t.name == name {
                return Some(t.to_owned());
            }
        }

        None
    }

    fn render_task_template(&self, task: &mut Task) -> Result<Vec<Map<String, Value>>> {
        let mut vec_params: Vec<Map<String, Value>> = Vec::new();
        let mut data: Map<String, Value> = Map::new();
        let component = task.name.as_str();

        data.insert("context".to_string(), Value::Object(self.context.clone()));
        data.insert("result".to_string(), Value::Object(self.result.clone()));

        expand_env_map(&mut data);

        if let Some(mut txt) = task.r#if.clone() {
            // Expand job if condition
            render_text_template(component, &mut txt, &data)?;

            match eval_boolean(&txt) {
                Ok(b) => {
                    if !b {
                        info!("{}", format!("task ignored: {}, if: {:?}, eval: {:?}", component, txt, b));
                        return Ok(vec_params);
                    }
                },
                Err(e) => return Err(anyhow!(e)),
            };
        }

        // Render loop if exists
        let mut r#loop: Value = Value::Null;
        if let Some(l) = task.r#loop.clone() {
            r#loop = render_loop_template(component, &l, &data)?;
        }

        if r#loop == Value::Null {
            let mut params = Map::new();

            for (n, v) in task.params.clone().into_iter() {
                params.insert(n.to_string(), render_value_template(component, &n, &v, &data)?);
            }

            vec_params.push(params);
        } else {
            let mut items: Vec<Value> = Vec::new();
            if let Some(its) = r#loop.as_array() {
                items = its.clone();
            };

            for (idx, it) in items.into_iter().enumerate() {
                data.insert("loop_item".to_string(), it);
                data.insert("loop_index".to_string(), Value::Number(Number::from(idx)));

                let mut params = Map::new();

                for (n, v) in task.params.clone().into_iter() {
                    params.insert(n.to_string(), render_value_template(component, &n, &v, &data)?);
                }

                vec_params.push(params);
            }
        }

        Ok(vec_params)
    }

    fn render_job_and_eval(&self) -> Result<bool> {
        if let Some(mut txt) = self.r#if.clone() {
            let component = self.name.as_str();
            let mut data: Map<String, Value> = Map::new();

            data.insert("context".to_string(), Value::Object(self.context.clone()));

            expand_env_map(&mut data);

            // Expand job if condition
            render_text_template(component, &mut txt, &data)?;

            match eval_boolean(&txt) {
                Ok(b) => {
                    if !b {
                        info!("{}", format!("job ignored: {}, if: {:?}, eval: {:?}", component, txt, b));
                        return Ok(false);
                    }

                    return Ok(true);
                },
                Err(e) => return Err(anyhow!(e)),
            };
        }

        Ok(true)
    }

    fn render_register(&mut self, reg_vars: &Map<String, Value>) -> Result<()> {
        let mut data: Map<String, Value> = Map::new();

        let mut vars = self.context
            .get("register")
            .and_then(|v| v.as_object())
            .unwrap_or(&Map::new()).to_owned();

        let msg_id = self.context
            .get("msg_id")
            .and_then(|v| v.as_object())
            .unwrap_or(&Map::new()).to_owned();

        let user_payload = self.context
            .get("user_payload")
            .and_then(|v| v.as_object())
            .unwrap_or(&Map::new()).to_owned();

        data.insert("result".to_string(), Value::Object(self.result.clone()));
        data.insert("msg_id".to_string(), Value::Object(msg_id));
        data.insert("user_payload".to_string(), Value::Object(user_payload));

        expand_env_map(&mut data);

        for (k, v) in reg_vars.clone().into_iter() {
            let val = render_value_template("register", &k, &v, &data)
                .and_then(|v| {
                    match v.as_str() {
                        Some(v) => match serde_json::from_str(v) {
                            Ok(v1) => Ok(v1),
                            Err(_) => Ok(Value::String(v.to_string()))
                        },
                        None => Ok(v),
                    }
                })?;


            vars.insert(k.to_string(), val);
        }

        self.context.insert("register".to_string(), Value::Object(vars));

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::Value as jsonValue;
    use serde_json::{Map, Number, json};
    use crate::plugin::{PluginRegistry, PluginExecResult};
    use crate::plugin_exec_result;

    #[tokio::test]
    async fn test_check_tasks() {
        let _ =  env_logger::try_init();
        PluginRegistry::load_plugins("target/debug").await;

        let mut job = Job::default();
        job.name = "job-1".to_string();
        job.hosts = "host1".to_string();

        let mut params_task1 = Map::new();
        params_task1.insert("cmd".to_string(), jsonValue::String("echo task1".to_string()));

        let mut params_task2 = Map::new();
        params_task2.insert("cmd".to_string(), jsonValue::String("echo task2".to_string()));

        let mut params_task3 = Map::new();
        params_task3.insert("cmd".to_string(), jsonValue::String("echo task3".to_string()));

        let mut params_task4 = Map::new();
        params_task4.insert("cmd".to_string(), jsonValue::String("echo task4".to_string()));

        let mut task1 = Task {
            name: "task-1".to_string(),
            r#if: None,
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            r#loop: None,
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let mut task2 = Task {
            name: "task-2".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task2.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let mut task3 = Task {
            name: "task-3".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task3.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task4.clone(),
            r#loop: None,
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        assert_eq!(job.check_tasks().unwrap_err().to_string(), "Plugin shell is not found");

        task1.plugin = "builtin-shell".to_string();
        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        assert_eq!(job.check_tasks().unwrap(), ());

        task3.on_failure = "helloworld".to_string();
        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        assert_eq!(job.check_tasks().unwrap_err().to_string(), "task helloworld is not found");

        task3.on_failure = "".to_string();
        task2.on_success = "helloworld".to_string();
        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        assert_eq!(job.check_tasks().unwrap_err().to_string(), "task helloworld is not found");
    }

    #[tokio::test]
    async fn test_run_all_tasks() {
        let _ =  env_logger::try_init();
        PluginRegistry::load_plugins("target/debug").await;

        let mut job = Job::default();
        job.name = "job-1".to_string();
        job.hosts = "localhost".to_string();

        let mut params_task1 = Map::new();
        params_task1.insert("cmd".to_string(), jsonValue::String("echo task1".to_string()));

        let mut params_task2 = Map::new();
        params_task2.insert("cmd".to_string(), jsonValue::String("echo task2".to_string()));

        let mut params_task3 = Map::new();
        params_task3.insert("cmd".to_string(), jsonValue::String("echo task3".to_string()));

        let mut params_task4 = Map::new();
        params_task4.insert("cmd".to_string(), jsonValue::String("echo task4".to_string()));

        let mut task1 = Task {
            name: "task-1".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            r#loop: None,
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let mut task2 = Task {
            name: "task-2".to_string(),
            r#if: Some("false".to_string()),
            plugin: "builtin-shell".to_string(),
            params: params_task2.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let mut task3 = Task {
            name: "task-3".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task3.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task4.clone(),
            r#loop: None,
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        job.run(None, None).await.unwrap();

        let mut expected = job_result!(
            "task-1" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task1\n".to_string()))).unwrap(),
            "task-4" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))).unwrap()
            );

        assert_eq!(expected, job.result);

        task1.on_success = "task-2".to_string();
        task2.on_success = "task-3".to_string();
        task2.r#if = None;
        task3.on_success = "task-4".to_string();

        expected = job_result!(
            "task-1" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task1\n".to_string()))).unwrap(),
            "task-2" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task2\n".to_string()))).unwrap(),
            "task-3" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task3\n".to_string()))).unwrap(),
            "task-4" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))).unwrap()
        );

        job.result.clear();

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];
        job.run(None, None).await.unwrap();

        assert_eq!(expected, job.result);

        params_task1.insert("cmd".to_string(), jsonValue::String("hello".to_string()));
        task1.params = params_task1.clone();
        task1.on_failure = "task-4".to_string();

        expected = job_result!(
            "task-1" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ko,
                        "No such file or directory (os error 2)",)).unwrap(),
            "task-4" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))).unwrap()
        );

        job.result.clear();

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];
        job.run(None, None).await.unwrap();

        assert_eq!(expected, job.result);

        // Test: Job condition
        job.r#if = Some("false".to_string());

        expected = job_result!();

        job.result.clear();
        job.run(None, None).await.unwrap();

        assert_eq!(expected, job.result);

    }

    #[tokio::test]
    async fn test_run_task_by_task() {
        let _ =  env_logger::try_init();
        PluginRegistry::load_plugins("target/debug").await;

        let mut job = Job::default();
        job.name = "job-1".to_string();
        job.hosts = "localhost".to_string();

        let mut params_task1 = Map::new();
        params_task1.insert("cmd".to_string(), jsonValue::String("echo task1".to_string()));

        let mut params_task2 = Map::new();
        params_task2.insert("cmd".to_string(), jsonValue::String("echo task2".to_string()));

        let mut params_task3 = Map::new();
        params_task3.insert("cmd".to_string(), jsonValue::String("echo task3".to_string()));

        let mut params_task4 = Map::new();
        params_task4.insert("cmd".to_string(), jsonValue::String("echo task4".to_string()));

        let mut task1 = Task {
            name: "task-1".to_string(),
            r#if: Some("false".to_string()),
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            r#loop: None,
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let task2 = Task {
            name: "task-2".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task2.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let task3 = Task {
            name: "task-3".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task3.clone(),
            r#loop: None,
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task4.clone(),
            r#loop: None,
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        job.run(Some("task-2,task-3"), None).await.unwrap();

        let mut expected = job_result!(
            "task-2" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task2\n".to_string()))).unwrap(),
            "task-3" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task3\n".to_string()))
        ).unwrap()
        );

        assert_eq!(expected, job.result);


        params_task1.insert("cmd".to_string(), jsonValue::String("hello".to_string()));
        task1.params = params_task1.clone();

        expected = job_result!(
            "task-4" => serde_json::to_value(plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))
        ).unwrap()
        );

        job.result.clear();

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];
        job.run(Some("task-1,task-4"), None).await.unwrap();

        assert_eq!(expected, job.result);
    }

    #[test]
    fn test_render_task_template() {
        let _ =  env_logger::try_init();

        let vars = json!({
            "var1": "${VAR1}",
            "var2": [
                "1",
                "${VAR21}",
                "3",
            ],
            "var3": [
                "var31",
                "${VAR32}",
                "var33",
            ],
            "var4": {
                "var41": {
                    "var411": "var411",
                    "var412": "${VAR412}"
                },
                "var42": "var42",
                "var43": "${VAR43}"
            },
            "superloop": [
                "${LOOP1}", "${LOOP2}"
            ]
        });

        envmnt::set("VAR1", "var1");
        envmnt::set("VAR21", "2");
        envmnt::set("VAR32", "var32");
        envmnt::set("VAR412", "var412");
        envmnt::set("VAR43", "var43");
        envmnt::set("LOOP1", "loop1");
        envmnt::set("LOOP2", "loop2");

        let mut job = Job::default();
        job.name = "job-1".to_string();
        job.hosts = "localhost".to_string();

        let mut params_task1 = Map::new();
        params_task1.insert("param1".to_string(), jsonValue::String("${VAR1} {{ context.variables.var1 }}".to_string()));
        params_task1.insert("param2".to_string(), jsonValue::Array(vec![Value::String("${VAR21}".to_string()), Value::String("{{ context.variables.var2.1 }}".to_string()), Value::String("3".to_string())]));
        params_task1.insert("param3".to_string(), jsonValue::String("${VAR412} {{ context.variables.var4.var41.var412 }}".to_string()));

        let mut task1 = Task {
            name: "task-1".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            r#loop: None,
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        job.tasks = vec![task1.clone()];
        job.context.insert("variables".to_string(), vars);

        let mut vec_params = job.render_task_template(&mut task1).unwrap();

        // Expected result
        let mut params_expected_1 = Map::new();
        params_expected_1.insert("param1".to_string(), jsonValue::String("var1 var1".to_string()));
        params_expected_1.insert("param2".to_string(), jsonValue::Array(vec![Value::String("2".to_string()), Value::String("2".to_string()), Value::String("3".to_string())]));
        params_expected_1.insert("param3".to_string(), jsonValue::String("var412 var412".to_string()));

        assert_eq!(vec![params_expected_1.clone()], vec_params);

        // Loop
        params_task1.insert("loop".to_string(), jsonValue::String("{{ loop_item }}".to_string()));

        task1.params = params_task1.clone();

        // Loop by a manual array
        task1.r#loop = Some(jsonValue::Array(vec![jsonValue::String("loop1".to_string()), jsonValue::String("loop2".to_string())]));

        vec_params = job.render_task_template(&mut task1).unwrap();

        // Expected result
        params_expected_1.insert("loop".to_string(), jsonValue::String("loop1".to_string()));

        let mut params_expected_2 = params_expected_1.clone();
        params_expected_2.insert("loop".to_string(), jsonValue::String("loop2".to_string()));

        assert_eq!(vec![params_expected_1.clone(), params_expected_2.clone()], vec_params);

        // Loop by rendering a variable array
        task1.r#loop = Some(jsonValue::String("{{ context.variables.superloop | json_encode() | safe }}".to_string()));
        vec_params = job.render_task_template(&mut task1).unwrap();

        assert_eq!(vec![params_expected_1.clone(), params_expected_2.clone()], vec_params);
    }
}
