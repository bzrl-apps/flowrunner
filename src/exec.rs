use clap::ArgMatches;

use anyhow::{anyhow, Result};
use tokio::signal;
use log::error;

use crate::config::Config;
use crate::flow::Flow;

pub async fn exec_cmd(config: &Config, matches: &ArgMatches<'_>) -> Result<()> {

    let file = match matches.value_of("flow-file") {
        Some(f) => f,
        None => return Err(anyhow!("You must specify the flow file in the specified flow directory (--flow-dir)")),
    };

    let mut flow = Flow::new_from_file(&(config.runner.flow_dir.as_str().to_owned() + "/" + file))?;

    tokio::spawn(async move {
        match flow.run().await {
            Ok(()) => (),
            Err(e) => { error!("{}", e.to_string()); },
        }
    });

    match signal::ctrl_c().await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!("Unable to listen for shutdown signal: {}", e);
            // we also shut down in case of error
            return Err(anyhow!(e));
        },
    }
}
