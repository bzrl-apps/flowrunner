use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use log::info;
use std::fs::File;

//use anyhow::Result;

use crate::job::Job;
use crate::inventory::Inventory;

#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub enum Status {
    Ko = 0,
    Ok = 1
}

#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Flow {
    #[serde(default)]
	pub variables:  HashMap<String, String>,
    #[serde(default)]
	pub jobs: Vec<Job>,
    #[serde(default)]
	pub inventory: Inventory,

    #[serde(default)]
	pub plugin_dir: String,
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

impl Flow {
    pub fn new_from_file(file: &str) {
        let f = File::open(file).unwrap();
        let mut mapping = serde_yaml::Mapping::new();

        mapping = serde_yaml::from_reader(f).unwrap();

        //Flow{}
    }

    pub fn new_from_str(content: &str) {
        let mut mapping = serde_yaml::Mapping::new();

        mapping = serde_yaml::from_str(content).unwrap();

        info!("mapping: {:?}", mapping);

        //Flow{}
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    #[test]
    fn test_new_from_str() {
        let content = r#"
variables:
  var1: var1
  var2: var2

jobs:
  - name: job1
    hosts: host1
    tasks:
    - shell:
        cmd: exec
        params:
          cmd: "hostname -f"

  - name: job2
    tasks:
    - shell:
        cmd: exec
        params:
          cmd: "hostname -f"

  - name: job3
    hosts: host2
    tasks:
    - shell:
        cmd: exec
        params:
          cmd: "hostname -f"
"#;

        new_from_str(content);
    }
} 
