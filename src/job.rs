use serde::{Deserialize, Serialize};
use anyhow::Result;
use std::collections::HashMap;

//use crate::cmd_registry::Cmd;
use crate::module::Module;

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Status {
    Ko = 0,
    Ok = 1,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Job {
	pub name: String,
	pub hosts: String,
	pub start: Task,

	pub tasks: Vec<Task>,
	pub context: HashMap<String, String>,

	pub status: Status,
	pub result: JobResult,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JobResult {
    pub name: String,
    pub tasks: HashMap<String, String>
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Task {
	pub name: String,
	pub module: Module,
	pub params: HashMap<String, String>,
	pub on_success: String,
	pub on_failure: String
}
