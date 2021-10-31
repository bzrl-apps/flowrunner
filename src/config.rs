use std::error::Error;
use std::fs::File;
//use std::path::Path;
use std::sync::Mutex;

use lazy_static::lazy_static;

use serde::{Deserialize, Serialize};

#[derive(Debug ,Serialize, Deserialize, PartialEq, Clone)]
pub struct Config {
    pub runner: RunnerConfig,
}

#[derive(Debug ,Serialize, Deserialize, Clone, PartialEq)]
pub struct RunnerConfig {
    #[serde(default)]
    pub plugin_dir: String,
    #[serde(default)]
    pub workflow_dir: String,
    #[serde(default)]
    pub job_parallel: bool
}

pub fn new(config_file: &str) -> Result<Config, Box<dyn Error>> {
    let f = File::open(config_file)?;
    let config: Config = serde_yaml::from_reader(f)?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_config_file() {
        let expected: Config = Config{
            runner: RunnerConfig{
                plugin_dir: "dist/plugins".to_string(),
                workflow_dir: "dist/workflows".to_string(),
                job_parallel: true
            }
        };

        let config = new(".flowrunner.yaml").unwrap();

        assert_eq!(expected, config)
    }
}
