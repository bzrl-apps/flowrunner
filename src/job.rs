use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::value::Value;

use anyhow::{Result, anyhow};
use std::collections::HashMap;

use log::{info, debug, error, warn};

use crate::plugin::{PluginRegistry, PluginExecResult, Status as PluginStatus};


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

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Status {
    Ko = 0,
    Ok = 1,
}

// Implement trait Default for Status
impl Default for Status {
    fn default() -> Self {
        Status::Ko
    }
}

impl Status {
    fn ko() -> Self { Status::Ko }
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
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

    #[serde(default = "Status::ko")]
	pub status: Status,
    #[serde(default)]
	pub result: HashMap<String, PluginExecResult>,
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

impl Job {
    pub async fn run(&mut self, tasks: &str) -> Result<()> {
        info!("JOB RUN STARTED: job: {}, hosts: {}", self.name, self.hosts);
        debug!("Job context: {:?}", self.context);

        // Check the name of all tasks indicated in taskflow
        self.check_tasks()?;

        // Run certain tasks given in parameter
        if tasks != "" {
            self.run_task_by_task(tasks).await?;
        } else {
            // Run complete taskflow by running the first task
            self.run_all_tasks(self.start.clone()).await?;
        }

        Ok(())
    }

    async fn run_all_tasks(&mut self, start: Option<Task>) -> Result<()> {
        let mut next_task: Option<Task> = match start {
            Some(task) => Some(task),
            None => Some(self.tasks[0].clone()),
        };

        let registry = PluginRegistry::get().lock().unwrap();
        // Execute task
        while let Some(t) = next_task.to_owned() {
            info!("Task executed: name {}, params {:?}", t.name, t.params);

            let res = registry.exec_plugin(&t.name, t.params).await;
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
        }

        Ok(())
    }

    async fn run_task_by_task(&mut self, tasks: &str) -> Result<()> {
        let registry = PluginRegistry::get().lock().unwrap();

        for s in tasks.split(",") {
            match self.get_task_by_name(s) {
                Some(t) => {
                    info!("Task executed: name {}, params {:?}", t.name, t.params);

                    let res = registry.exec_plugin(&t.name, t.params).await;
                    self.result.insert(t.name.clone(), res.clone());

                    info!("Task result: name {}, res: {:?}",  t.name.clone(), res);
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

}


//impl TaskResult {
    //pub fn new() -> Self {
        //TaskResult::default()
    //}
//}

//impl Task {
    //pub fn new() -> Self {
        //Task::default()
    //}
//}
//
#[cfg(test)]
mod tests {
    use super::*;
    use flowrunner::plugin::Plugin;
    use serde_json::value::Value as jsonValue;
    use serde_json::{Map, Number};
    use crate::plugin::PluginRegistry;

    #[test]
    fn test_check_tasks() {
        env_logger::init();

        let mut job = Job {
            name: "job-1".to_string(),
            hosts: "host1".to_string(),
            start: None,
            tasks: vec![],
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
        };

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
        PluginRegistry::load_plugins("target/debug");

        let mut job = Job {
            name: "job-1".to_string(),
            hosts: "localhost".to_string(),
            start: None,
            tasks: vec![],
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
        };

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

        job.run("").await.unwrap();

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
        job.run("").await.unwrap();

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
        job.run("").await.unwrap();

        assert_eq!(expected, job.result);
    }

    #[tokio::test]
    async fn test_run_task_by_task() {
        env_logger::init();
        PluginRegistry::load_plugins("target/debug");

        let mut job = Job {
            name: "job-1".to_string(),
            hosts: "localhost".to_string(),
            start: None,
            tasks: vec![],
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
        };

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

        job.run("task-2,task-3").await.unwrap();

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
        job.run("task-1,task-4").await.unwrap();

        assert_eq!(expected, job.result);
    }
}
