use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::str::FromStr;

use log::{info, debug, error, warn};
use envmnt::{ExpandOptions, ExpansionType};
use tera::{Tera, Context};

//use tokio::sync::mpsc::*;
use async_channel::*;

use crate::plugin::{PluginRegistry, PluginExecResult, Status as PluginStatus};
use crate::message::Message as FlowMessage;

#[macro_export]
macro_rules! job_result {
    ($( $key:expr => $val:expr ), *) => {
        {
            let mut result = HashMap::<String, PluginExecResult>::new();
            $( result.insert($key.to_string(), $val); )*
            result
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    #[serde(default)]
	pub name: String,
    #[serde(default)]
	pub hosts: String,
    #[serde(default)]
	pub start: Option<Task>,

    #[serde(default)]
	pub tasks: Vec<Task>,
    #[serde(default)]
	pub context: Map<String, Value>,

    #[serde(default)]
	pub result: HashMap<String, PluginExecResult>,

    #[serde(skip_serializing, skip_deserializing)]
	pub rx: Vec<Sender<FlowMessage>>,
    #[serde(skip_serializing, skip_deserializing)]
	pub tx: Vec<Receiver<FlowMessage>>
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Task {
    #[serde(default)]
	pub name: String,
    #[serde(default)]
    pub plugin: String,
    #[serde(default)]
	pub params: Map<String, Value>,
    #[serde(default)]
	pub on_success: String,
    #[serde(default)]
	pub on_failure: String
}

//impl Clone for Job {
    //fn clone(&self) -> Self {
        //Job {
            //name: self.name.clone(),
            //hosts: self.hosts.clone(),
            //start: self.start.clone(),
            //tasks: self.tasks.clone(),
            //context: self.context.clone(),
            //status: self.status.clone(),
            //result: self.result.clone(),
            //rx: vec![],
            //tx: vec![],
        //}
    //}
//}

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
    pub async fn run(&mut self, tasks: Option<&str>) -> Result<()> {
        info!("JOB RUN STARTED: job: {}, hosts: {}", self.name, self.hosts);
        debug!("Job context: {:?}", self.context);

        // Check the name of all tasks indicated in taskflow
        self.check_tasks()?;

        let ts = match tasks {
            Some(s) => s,
            None => "",
        };

        if self.tx.len() > 0 {
            loop {
                match self.tx[0].recv().await {
                    // Add message received as data in job context
                    Ok(msg) => {
                        match msg {
                            FlowMessage::Json(v) => { self.context.insert("data".to_string(), v); },
                            _ => {
                                error!("Message received is not Message::Json type");
                                continue;
                            },
                       }

                        // Run certain tasks given in parameter
                        if ts != "" {
                            self.run_task_by_task(ts).await?;
                        } else {
                            // Run complete taskflow by running the first task
                            self.run_all_tasks(self.start.clone()).await?;
                        }
                    },
                    Err(e) => { error!("{}", e.to_string()); break; },
                }
            }
        } else {
            // Run certain tasks given in parameter
            if ts != "" {
                self.run_task_by_task(ts).await?;
            } else {
                // Run complete taskflow by running the first task
                self.run_all_tasks(self.start.clone()).await?;
            }
        }

        Ok(())
    }

    async fn run_all_tasks(&mut self, start: Option<Task>) -> Result<()> {
        let mut next_task: Option<Task> = match start {
            Some(task) => Some(task),
            None => Some(self.tasks[0].clone()),
        };

        // Execute task
        while let Some(mut t) = next_task.to_owned() {
            info!("Task executed: name {}, params {:?}", t.name, t.params);

            self.render_task_template(&mut t)?;

            match PluginRegistry::get_plugin(&t.plugin) {
                Some(mut plugin) => {
                    plugin.validate_params(t.params.clone())?;
                    let res = plugin.func(&self.rx, &self.tx).await;
                    self.result.insert(t.name.clone(), res.clone());

                    info!("Task result: name {}, res: {:?}",  t.name.clone(), res);

                    if res.status == PluginStatus::Ko {
                        // Go the task of Success if specified
                        next_task = match t.on_failure.len() {
                            n if n <= 0 => return Err(anyhow!(res.error)),
                            _ => self.get_task_by_name(t.on_failure.as_str()),
                        };

                        continue;
                    }

                    next_task = match t.on_success.len() {
                        n if n > 0 => self.get_task_by_name(t.on_success.as_str()),
                        _ => None,
                    };
                },
                None => error!("No plugin found"),
            }
        }

        Ok(())
    }

    async fn run_task_by_task(&mut self, tasks: &str) -> Result<()> {
        for s in tasks.split(",") {
            match self.get_task_by_name(s) {
                Some(t) => {
                    info!("Task executed: name {}, params {:?}", t.name, t.params);

                    match PluginRegistry::get_plugin(&t.plugin) {
                        Some(mut plugin) => {
                            plugin.validate_params(t.params.clone())?;
                            let res = plugin.func(&self.rx, &self.tx).await;
                            self.result.insert(t.name.clone(), res.clone());

                            info!("Task result: name {}, res: {:?}",  t.name.clone(), res);
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
            if t.on_failure != "".to_string() {
                if map.contains_key(&t.on_failure) != true {
                    return Err(anyhow!("task ".to_owned() + &t.on_failure + " is not found"));
                }
            }

            if t.on_failure != "".to_string() {
                if map.contains_key(&t.on_success) != true {
                    return Err(anyhow!("task ".to_owned() + &t.on_success + " is not found"));
                }
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

    fn render_task_template(&self, task: &mut Task) -> Result<()> {
        let mut data: Map<String, Value> = Map::new();

        data.insert("context".to_string(), Value::from(self.context.clone()));

        // Convert job'Vs result HashMap to serde_json Map
        let mut job_result: Map<String, Value> = Map::new();

        for (n, r) in self.result.iter() {
            let r_value = Value::from(serde_json::to_string(r)?);
            job_result.insert(n.to_string(), r_value);
        }

        data.insert("result".to_string(), Value::from(job_result));

        expand_env_map(&mut data);

        // Expand task's params
        for (n, v) in task.params.clone().into_iter() {
            task.params.insert(n.to_string(), render_param_template(task.name.as_str(), &n, &v, &data)?);
        }

        Ok(())
    }

}

fn expand_env_map(m: &mut Map<String, Value>) {
    for (k, v) in m.clone().into_iter() {
        m.insert(k, expand_env_value(&v));
    }
}

fn expand_env_value(value: &Value) -> Value {
    let mut options = ExpandOptions::new();
    options.expansion_type = Some(ExpansionType::Unix);

    match value {
        Value::Array(arr) => {
            let mut v: Vec<Value> = vec![];
            for e in arr.into_iter() {
                let expanded_v = expand_env_value(e);
                v.push(expanded_v);
            }

            return Value::Array(v);
        },
        Value::Object(map) => {
            let mut m: Map<String, Value> = Map::new();
            for (k, v) in map.into_iter() {
                let expanded_v = expand_env_value(v);
                m.insert(k.to_string(), expanded_v);
            }

            return Value::Object(m);
        },
        _ => Value::String(envmnt::expand(value.as_str().unwrap_or(""), Some(options))),
    }
}

fn render_param_template(task: &str, key: &str, value: &Value, data: &Map<String, Value>) -> Result<Value> {
    debug!("Rendering param templating: task {}, key {}, value {:?}, data {:?}", task, key, value, data);

    let mut tera = Tera::default();
    let exp_env_v = expand_env_value(value);

    let context = match Context::from_value(Value::Object(data.to_owned())) {
        Ok(c) => c,
        Err(e) => return Err(anyhow!(e)),
    };

    match exp_env_v {
        Value::Array(arr) => {
            let mut v: Vec<Value> = vec![];
            for e in arr.into_iter() {
                let rendered_v = render_param_template(task, key, &e, data)?;
                v.push(rendered_v);
            }

            return Ok(Value::Array(v));
        },
        Value::Object(map) => {
            let mut m: Map<String, Value> = Map::new();
            for (k, v) in map.into_iter() {
                let rendered_v = render_param_template(task, key, &v, data)?;
                m.insert(k.to_string(), rendered_v);
            }

            return Ok(Value::Object(m));
        },
        _ => {
            match tera.render_str(exp_env_v.as_str().unwrap_or(""), &context) {
                Ok(s) => Ok(Value::String(s)),
                Err(e) => Err(anyhow!(e))
            }
        },
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::Value as jsonValue;
    use serde_json::{Map, Number, json};
    use crate::plugin::PluginRegistry;

    #[test]
    fn test_check_tasks() {
        env_logger::init();

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

        let task1 = Task {
            name: "task-1".to_string(),
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let mut task2 = Task {
            name: "task-2".to_string(),
            plugin: "shell".to_string(),
            params: params_task2.clone(),
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let mut task3 = Task {
            name: "task-3".to_string(),
            plugin: "shell".to_string(),
            params: params_task3.clone(),
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
                plugin: "shell".to_string(),
            params: params_task4.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

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
        env_logger::init();
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
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let mut task2 = Task {
            name: "task-2".to_string(),
            plugin: "shell".to_string(),
            params: params_task2.clone(),
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let mut task3 = Task {
            name: "task-3".to_string(),
            plugin: "shell".to_string(),
            params: params_task3.clone(),
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            plugin: "shell".to_string(),
            params: params_task4.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        job.run(None).await.unwrap();

        let mut expected = job_result!(
            "task-1" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task1\n".to_string())),
            "task-2" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task2\n".to_string())),
            "task-4" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))
        );

        assert_eq!(expected, job.result);

        task1.on_success = "task-2".to_string();
        task2.on_success = "task-3".to_string();
        task3.on_success = "task-4".to_string();

        expected = job_result!(
            "task-1" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task1\n".to_string())),
            "task-2" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task2\n".to_string())),
            "task-3" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task3\n".to_string())),
            "task-4" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))
        );

        job.result.clear();

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];
        job.run(None).await.unwrap();

        assert_eq!(expected, job.result);

        params_task1.insert("cmd".to_string(), jsonValue::String("hello".to_string()));
        task1.params = params_task1.clone();
        task1.on_failure = "task-4".to_string();

        expected = job_result!(
            "task-1" => plugin_exec_result!(
                        PluginStatus::Ko,
                        "No such file or directory (os error 2)",),
            "task-4" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))
        );

        job.result.clear();

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];
        job.run(None).await.unwrap();

        assert_eq!(expected, job.result);
    }

    #[tokio::test]
    async fn test_run_task_by_task() {
        env_logger::init();
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
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let task2 = Task {
            name: "task-2".to_string(),
            plugin: "shell".to_string(),
            params: params_task2.clone(),
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let task3 = Task {
            name: "task-3".to_string(),
            plugin: "shell".to_string(),
            params: params_task3.clone(),
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            plugin: "shell".to_string(),
            params: params_task4.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        job.run(Some("task-2,task-3")).await.unwrap();

        let mut expected = job_result!(
            "task-2" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task2\n".to_string())),
            "task-3" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task3\n".to_string()))
        );

        assert_eq!(expected, job.result);


        params_task1.insert("cmd".to_string(), jsonValue::String("hello".to_string()));
        task1.params = params_task1.clone();

        expected = job_result!(
            "task-1" => plugin_exec_result!(
                        PluginStatus::Ko,
                        "No such file or directory (os error 2)",),
            "task-4" => plugin_exec_result!(
                        PluginStatus::Ok,
                        "",
                        "rc" => jsonValue::Number(Number::from(0)),
                        "stdout" => jsonValue::String("task4\n".to_string()))
        );

        job.result.clear();

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];
        job.run(Some("task-1,task-4")).await.unwrap();

        assert_eq!(expected, job.result);
    }

    #[test]
    fn test_expand_env_map() {
        env_logger::init();

        let input = json!({
            "var1": "$VAR1",
            "var2": [
                "1",
                "$VAR21",
                "3",
            ],
            "var3": [
                "var31",
                "$VAR32",
                "var33",
            ],
            "var4": {
                "var41": {
                    "var411": "var411",
                    "var412": "${VAR412}"
                },
                "var42": "var42",
                "var43": "$VAR43"
            }
        });

        envmnt::set("VAR1", "var1");
        envmnt::set("VAR21", "2");
        envmnt::set("VAR32", "var32");
        envmnt::set("VAR412", "var412");
        envmnt::set("VAR43", "var43");

        let mut expanded_m = input.as_object().unwrap().to_owned();
        expand_env_map(&mut expanded_m);

        let expected = json!({
            "var1": "var1",
            "var2": [
                "1",
                "2",
                "3",
            ],
            "var3": [
                "var31",
                "var32",
                "var33",
            ],
            "var4": {
                "var41": {
                    "var411": "var411",
                    "var412": "var412"
                },
                "var42": "var42",
                "var43": "var43"
            }
        });

        assert_eq!(expected.as_object().unwrap(), &expanded_m);
    }

    #[test]
    fn test_render_task_template() {
        env_logger::init();

        let vars = json!({
            "var1": "$VAR1",
            "var2": [
                "1",
                "$VAR21",
                "3",
            ],
            "var3": [
                "var31",
                "$VAR32",
                "var33",
            ],
            "var4": {
                "var41": {
                    "var411": "var411",
                    "var412": "${VAR412}"
                },
                "var42": "var42",
                "var43": "$VAR43"
            }
        });

        envmnt::set("VAR1", "var1");
        envmnt::set("VAR21", "2");
        envmnt::set("VAR32", "var32");
        envmnt::set("VAR412", "var412");
        envmnt::set("VAR43", "var43");

        let mut job = Job::default();
        job.name = "job-1".to_string();
        job.hosts = "localhost".to_string();

        let mut params_task1 = Map::new();
        params_task1.insert("param1".to_string(), jsonValue::String("$VAR1 {{ context.variables.var1 }}".to_string()));
        params_task1.insert("param2".to_string(), jsonValue::Array(vec![Value::String("$VAR21".to_string()), Value::String("{{ context.variables.var2.1 }}".to_string()), Value::String("3".to_string())]));
        params_task1.insert("param3".to_string(), jsonValue::String("$VAR412 {{ context.variables.var4.var41.var412 }}".to_string()));

        let mut task1 = Task {
            name: "task-1".to_string(),
            plugin: "shell".to_string(),
            params: params_task1.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        job.tasks = vec![task1.clone()];
        job.context.insert("variables".to_string(), vars);

        job.render_task_template(&mut task1).unwrap();

        // Expected result
        let mut params_expected = Map::new();
        params_expected.insert("param1".to_string(), jsonValue::String("var1 var1".to_string()));
        params_expected.insert("param2".to_string(), jsonValue::Array(vec![Value::String("2".to_string()), Value::String("2".to_string()), Value::String("3".to_string())]));
        params_expected.insert("param3".to_string(), jsonValue::String("var412 var412".to_string()));

        let mut expected = Task {
            name: "task-1".to_string(),
            plugin: "shell".to_string(),
            params: params_expected.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        assert_eq!(expected, task1);
    }
}
