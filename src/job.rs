use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Status {
    Ko = 0,
    Ok = 1,
}

#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Job {
	pub name: String,
	pub hosts: String,
	pub start: Task,

	pub tasks: Vec<Task>,
	pub context: HashMap<String, String>,

	pub status: Status,
	pub result: HashMap<String, Result<HashMap<String, String>>>
}

pub struct Task {
	pub name: String,
	pub cmd: Cmd,
	pub params: HashMap<String, String>,
	pub onSuccess: String,
	pub onFailure: String
}
