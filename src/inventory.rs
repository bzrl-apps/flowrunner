use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Describes structure of a host
#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Host {
	pub name: String,

    pub groups: Vec<String>,
	pub vars: HashMap<String, String>
}

/// Describes attributes of a group
#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Group {
	pub name: String,

	pub hosts: Vec<String>,
	pub vars: HashMap<String, String>
}

/// Describes attributes of a host inventory
#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Inventory {
	pub global: HashMap<String, String>,
	pub hosts: HashMap<String, Host>,
	pub groups: HashMap<String, Group>
}

// Implement trait Default for structs
impl Default for Inventory {
    fn default() -> Self {
        Inventory {
            global: HashMap::new(),
            hosts: HashMap::new(),
            groups: HashMap::new()
        }
    }
}

////////// DEFINITION OF ALL FUNCTIONS ///////////////////////////

// NewInventory instancies a new Inventory
//impl Inventory {
    //pub fn new() -> Self {
        //Inventory {
            //global: HashMap::new(),
            //hosts: HashMap::new(),
            //groups: HashMap::new()
        //}
    //}
//}
