use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use anyhow::Result;
use log::warn;

pub type CmdFunc = fn(params: HashMap<String, String>) -> Result<HashMap<String, String>>;

#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Module {
	pub name: String,
	pub version: String,
	pub description: String
}

#[derive(Debug, PartialEq, Clone)]
pub struct Cmd {
	pub name: String,
	pub func: CmdFunc,
	pub module: Module
}

#[derive(Debug, PartialEq, Clone)]
pub struct CmdRegistry {
	pub cmds: HashMap<String, Cmd>
}

/// Implement methods for CmdRegistry
impl CmdRegistry {
    /// Instanciate a new command registry
    pub fn new() -> Self {
        CmdRegistry{cmds: HashMap::new()}
    }

    /// Register a new command in a hash map with the name:  modulename.cmdname
    pub fn register(&mut self, cmd: Cmd) {
        let fullname = cmd.module.name.clone() + "." + cmd.name.as_str();

        let res = self.cmds.insert(fullname.to_owned(), cmd);

        if !res.is_none() {
            warn!("{} already exists. Its value will be updated", fullname);
        }
    }

    /// Unregister a command
    pub fn unregister(&mut self, cmd: Cmd) {
        let fullname = cmd.module.name.clone() + "." + cmd.name.as_str();

        let res = self.cmds.remove(&fullname);

        if res.is_none() {
            warn!("{} not found", fullname);
        }
    }

    /// Get the number of commands in the registry
    pub fn get_nb_of_cmds(&self) -> usize {
        self.cmds.len()
    }

    /// Get a command by its name
    pub fn get_cmd_by_name(&self, name: &str) -> Option<Cmd> {
        self.cmds.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;
    use log::info;

    fn func(params: HashMap<String, String>) -> Result<HashMap<String, String>> {
        Ok(HashMap::new())
    }

    #[test]
    fn test_cmd_registry() {
        let func: CmdFunc = func;

        let module = Module{
            name: "ModTest".to_string(),
            version: "0.1.0".to_string(),
            description: "ModTest".to_string()
        };

        let mut registry = CmdRegistry::new();

        let cmd1 = Cmd{
            name: "cmd1".to_string(),
            func,
            module: module.clone()
        };

        registry.register(cmd1);
        assert_eq!(1, registry.get_nb_of_cmds());

        let cmd2 = Cmd{
            name: "cmd2".to_string(),
            func,
            module: module.clone()
        };

        registry.register(cmd2.to_owned());
        assert_eq!(2, registry.get_nb_of_cmds());

        let cmd3 = Cmd{
            name: "cmd3".to_string(),
            func,
            module: module.clone()
        };

        registry.register(cmd3);
        assert_eq!(3, registry.get_nb_of_cmds());

        let c1 = registry.get_cmd_by_name("ModTest.cmd2").unwrap();
        assert_eq!(c1, cmd2);

        registry.unregister(cmd2.to_owned());

        let c2 = registry.get_cmd_by_name("ModTest.cmd2");
        assert_eq!(None, c2);
    }
}
