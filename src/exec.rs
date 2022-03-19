use clap::ArgMatches;

use anyhow::{anyhow, Result};
use tokio::signal;
use log::{error, info};

use crate::config::Config;
use crate::flow::{Flow, Kind};

pub async fn exec_cmd(config: &Config, matches: &ArgMatches<'_>) -> Result<()> {

    let file = match matches.value_of("flow-file") {
        Some(f) => f,
        None => return Err(anyhow!("You must specify the flow file in the specified flow directory (--flow-dir)")),
    };

    let mut flow = Flow::new_from_file(&(config.runner.flow_dir.as_str().to_owned() + "/" + file))?;

    let kind = flow.kind;

    // flow == stream
    if kind == Kind::Stream {
        tokio::spawn(async move {
            match flow.run().await {
                Ok(()) => (),
                Err(e) => { error!("{}", e.to_string()); },
            }
        });

        match signal::ctrl_c().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                error!("Unable to listen for shutdown signal: {}", e);
                // we also shut down in case of error
                return Err(anyhow!(e));
            },
        };
    }

    // flow == action
    match flow.run().await {
        Ok(()) => {
            info!("Flow: {}", serde_json::to_string_pretty(&flow).unwrap_or_else(|_| "Cannot to serialize flow to string".to_string()));
        },
        Err(e) => error!("{e}"),
    }

    Ok(())
}
