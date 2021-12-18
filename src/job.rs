use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::value::Value;

use anyhow::{Result, anyhow};
use std::collections::HashMap;

use log::{info, debug, error};

//use crate::cmd_registry::Cmd;

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
	pub result: JobResult,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JobResult {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub tasks: Map<String, Value>
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
    pub fn run(&mut self, tasks: &str) -> Result<()> {
        info!("JOB RUN STARTED: job: {}, hosts: {}", self.name, self.hosts);
        debug!("Job context: {:?}", self.context);

        // Check the name of all tasks indicated in taskflow
        self.check_tasks()?;

        // Run certain tasks given in parameter
        if tasks != "" {
            self.run_task_by_task(tasks)?;
        } else {
            // Run complete taskflow by running the first task
            self.run_all_tasks(self.start.clone())?;
        }

        Ok(())
    }

    pub fn run_all_tasks(&mut self, start: Option<Task>) -> Result<()> {
        let t = match start {
            Some(task) => task,
            None => self.tasks[0].clone(),
        };


        //debug!("Job result: {:?}", self.result)
        //info!("JOB RUN COMPLETED: job {}, hosts: {}", self.name, self.hosts)
        Ok(())
    }

    pub fn run_task_by_task(&mut self, tasks: &str) -> Result<()> {
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

}


//impl JobResult {
    //pub fn new() -> Self {
        //JobResult::default()
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
    use serde_json::value::Value as jsonValue;

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
            result: JobResult::default(),
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

        let mut task4 = Task {
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
}
