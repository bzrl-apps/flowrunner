use std::collections::HashMap;
use log::info;
use std::fs::File;

use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value as yamlValue};
use serde_json::{Map, Number};
use serde_json::value::Value as jsonValue;

use anyhow::{anyhow, Result};

use crate::{job::{Task, Job}, utils};

//use crate::inventory::Inventory;

#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Flow {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
	pub variables:  Map<String, jsonValue>,
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

    //#[serde(default)]
	//pub status: Status,
    #[serde(default)]
	pub result: HashMap<String, Job>
}

impl Default for Flow {
    fn default() -> Self {
        Flow {
            name: "".to_string(),
            variables: Map::new(),
            jobs: vec![],
            remote_plugin_dir: "".to_string(),
            remote_exec_dir: "".to_string(),
            inventory_file: "".to_string(),
            is_on_remote: false,
            result: HashMap::new()
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

    pub fn run_all_jobs(&mut self) -> Result<()> {
        let mut jobs = self.jobs.clone();

        for (i, j) in self.jobs.iter().enumerate() {
            info!("Executing job {}", j.name);

            exec_job(&mut jobs, i)?;
        }

        self.jobs = jobs;

        Ok(())
    }

    pub fn run_job(&mut self) {}
}

fn exec_job(jobs: &mut [Job], idx: usize) -> Result<()> {
    let mut job = jobs[idx].clone();

    if job.hosts == "".to_string() || job.hosts == "localhost".to_string() || job.hosts == "127.0.0.1".to_string() {
        let _ = exec_job_local(&mut job)?;
        jobs[idx] = job;

        return Ok(());
    }

   let _ = exec_job_remote(&mut job)?;
   jobs[idx] = job;

   Ok(())
}

fn exec_job_local(job: &mut Job) -> Result<()> {
    info!("Executing locally the job {}", job.name);



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

    if let Some(variables) = mapping.get(&yamlValue::String("variables".to_string())) {
        if let Some(vars) = variables.as_mapping() {
            for (k, v) in vars.iter() {
                let key = k.as_str().ok_or(anyhow!("Error parsing variables' key"))?;
                let val = utils::convert_value_yaml_to_json(v)?;
                flow.variables.insert(key.to_string(), val);
            }
        }
    }

    flow.jobs = match mapping.get(&yamlValue::String("jobs".to_string())) {
        Some(js) => {
            match js.as_sequence() {
                Some(seq) => {
                    let mut jobs: Vec<Job> = Vec::new();

                    for job in seq.iter() {
                        let mut j = Job::default();
                        let mut job_count = 1;

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

                                            if let Some(v3) = v.get(yamlValue::String("params".to_string())) {
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
    use crate::job::Status;
    use super::*;

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
        });

        jobs.push(Job {
            name: "job2".to_string(),
            hosts: "localhost".to_string(),
            start: None,
            tasks: job2_tasks,
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
        });

        jobs.push(Job {
            name: "job3".to_string(),
            hosts: "host3".to_string(),
            start: None,
            tasks: job3_tasks,
            context: Map::new(),
            status: Status::Ko,
            result: HashMap::new(),
        });

        let expected = Flow {
            name: "flow1".to_string(),
            variables,
            jobs,
            remote_plugin_dir: "".to_string(),
            remote_exec_dir: "".to_string(),
            inventory_file: "".to_string(),
            is_on_remote: false,
            result: HashMap::new(),
        };

        assert_eq!(flow.unwrap(), expected);
    }

    #[test]
    fn test_run_all_jobs() {
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

        let mut flow =  Flow::new_from_str(content).unwrap();

        flow.run_all_jobs();
    }
}
