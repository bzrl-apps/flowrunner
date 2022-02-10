use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;

use anyhow::{Result, anyhow};
use std::collections::HashMap;

use evalexpr::*;

use log::{info, debug, error, warn};

//use tokio::sync::mpsc::*;
use async_channel::*;

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

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
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
	pub tasks: Vec<Task>,
    #[serde(default)]
	pub context: Map<String, Value>,

    #[serde(default)]
	pub result: Map<String, Value>,

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
    pub r#if: Option<String>,
    #[serde(default)]
    pub plugin: String,
    #[serde(default)]
	pub params: Map<String, Value>,

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
    pub async fn run(&mut self, tasks: Option<&str>) -> Result<()> {
        info!("JOB RUN STARTED: job: {}, hosts: {}, nb rx: {}, nb tx: {}", self.name, self.hosts, self.rx.len(), self.tx.len());
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
                            FlowMessage::JsonWithSender{ sender: s, source: src, value: v } => {
                                self.context.insert("sender".to_string(), Value::String(s));
                                self.context.insert("source".to_string(), Value::String(src.unwrap_or("".to_string())));
                                self.context.insert("data".to_string(), v);
                            },
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

                        for rx1 in self.rx.iter() {
                            let msg = FlowMessage::JsonWithSender {
                                sender: self.name.clone(),
                                source: None,
                                value: Value::Object(self.result.clone()),
                            };
                            match rx1.send(msg).await {
                                Ok(()) => (),
                                Err(e) => error!("{}", e.to_string()),
                            };
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

        // If job condition is not satisfied, then exit
        if !self.render_job_and_eval()? {
            return Ok(());
        }

        // Execute task
        while let Some(mut t) = next_task.to_owned() {
            info!("Task executed: name {}, params {:?}", t.name, t.params);

            // If task condition is not satisfied, then move to next task as when the task is
            // correctly executed
            if !self.render_task_template(&mut t)? {
                next_task = match t.on_success.len() {
                    n if n > 0 => self.get_task_by_name(t.on_success.as_str()),
                    _ => None,
                };

                continue;
            }

            match PluginRegistry::get_plugin(&t.plugin) {
                Some(mut plugin) => {
                    plugin.validate_params(t.params.clone())?;
                    let res = plugin.func(Some(self.name.clone()), &self.rx, &self.tx).await;
                    self.result.insert(t.name.clone(), serde_json::to_value(res.clone())?);

                    info!("Task result: name {}, res: {:?}",  t.name.clone(), res);

                    if res.status == PluginStatus::Ko {
                        // Go the task of Success if specified
                        next_task = match t.on_failure.len() {
                            n if n <= 0 => return Err(anyhow!(res.error)),
                            _ => self.get_task_by_name(t.on_failure.as_str()),
                        };

                        continue;
                    }
                },
                None => error!("No plugin found"),
            };

             // Move to next task on success when PluginStatus::Ok or no plugin found
            next_task = match t.on_success.len() {
                n if n > 0 => self.get_task_by_name(t.on_success.as_str()),
                _ => None,
            };
        }

        Ok(())
    }

    async fn run_task_by_task(&mut self, tasks: &str) -> Result<()> {
        // If job condition is not satisfied then exit
        if !self.render_job_and_eval()? {
            return Ok(())
        }

        for s in tasks.split(",") {
            match self.get_task_by_name(s) {
                Some(mut t) => {
                    info!("Task executed: name {}, params {:?}", t.name, t.params);

                    // If task condition is not satisfied then move to next one
                    if !self.render_task_template(&mut t)? {
                        continue
                    }

                    match PluginRegistry::get_plugin(&t.plugin) {
                        Some(mut plugin) => {
                            plugin.validate_params(t.params.clone())?;
                            let res = plugin.func(Some(self.name.clone()), &self.rx, &self.tx).await;
                            self.result.insert(t.name.clone(), serde_json::to_value(res.clone())?);

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
            if PluginRegistry::get_plugin(&t.plugin).is_none() {
                return Err(anyhow!("{}", format!("Plugin {} is not found", t.plugin)));
            }

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

    fn render_task_template(&self, task: &mut Task) -> Result<bool> {
        let mut data: Map<String, Value> = Map::new();
        let component = task.name.as_str();

        data.insert("context".to_string(), Value::Object(self.context.clone()));

        // Convert job'Vs result HashMap to serde_json Map
        let mut job_result: Map<String, Value> = Map::new();

        for (n, r) in self.result.iter() {
            let r_value = Value::from(serde_json::to_string(r)?);
            job_result.insert(n.to_string(), r_value);
        }

        data.insert("result".to_string(), Value::from(job_result));

        expand_env_map(&mut data);

        if let Some(mut txt) = task.r#if.clone() {
            // Expand job if condition
            render_text_template(component, &mut txt, &data)?;

            match eval_boolean(&txt) {
                Ok(b) => {
                    if !b {
                        debug!("{}", format!("task ignored: {}, if: {:?}, eval: {:?}", component, txt, b));
                        return Ok(false);
                    }

                    return Ok(true)
                },
                Err(e) => return Err(anyhow!(e)),
            };
        }

        // Expand task's params
        for (n, v) in task.params.clone().into_iter() {
            task.params.insert(n.to_string(), render_param_template(component, &n, &v, &data)?);
        }

        Ok(true)
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
                        debug!("{}", format!("job ignored: {}, if: {:?}, eval: {:?}", component, txt, b));
                        return Ok(false);
                    }

                    return Ok(true);
                },
                Err(e) => return Err(anyhow!(e)),
            };
        }

        Ok(true)
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
        env_logger::init();
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
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let mut task2 = Task {
            name: "task-2".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task2.clone(),
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let mut task3 = Task {
            name: "task-3".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task3.clone(),
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task4.clone(),
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
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let mut task2 = Task {
            name: "task-2".to_string(),
            r#if: Some("false".to_string()),
            plugin: "builtin-shell".to_string(),
            params: params_task2.clone(),
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let mut task3 = Task {
            name: "task-3".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task3.clone(),
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task4.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        job.run(None).await.unwrap();

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
        job.run(None).await.unwrap();

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
        job.run(None).await.unwrap();

        assert_eq!(expected, job.result);

        // Test: Job condition
        job.r#if = Some("false".to_string());

        expected = job_result!();

        job.result.clear();
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
            r#if: Some("false".to_string()),
            plugin: "builtin-shell".to_string(),
            params: params_task1.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        let task2 = Task {
            name: "task-2".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task2.clone(),
            on_success: "task-4".to_string(),
            on_failure: "task-4".to_string()
        };

        let task3 = Task {
            name: "task-3".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task3.clone(),
            on_success: "task-4".to_string(),
            on_failure: "".to_string()
        };

        let task4 = Task {
            name: "task-4".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_task4.clone(),
            on_success: "".to_string(),
            on_failure: "".to_string()
        };

        job.tasks = vec![task1.clone(), task2.clone(), task3.clone(), task4.clone()];

        job.run(Some("task-2,task-3")).await.unwrap();

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
        job.run(Some("task-1,task-4")).await.unwrap();

        assert_eq!(expected, job.result);
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
            r#if: None,
            plugin: "builtin-shell".to_string(),
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

        let expected = Task {
            name: "task-1".to_string(),
            r#if: None,
            plugin: "builtin-shell".to_string(),
            params: params_expected.clone(),
            on_success: "task-2".to_string(),
            on_failure: "task-3".to_string()
        };

        assert_eq!(expected, task1);
    }
}
