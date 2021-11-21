use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::value::Value;

use anyhow::Result;
use std::collections::HashMap;

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

//impl Job {
    //pub fn new() -> Self {
        //Job::default()
    //}
//}

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
