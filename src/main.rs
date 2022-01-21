#[macro_use]
extern crate lazy_static;
extern crate log;
extern crate clap;

use log::{info, error};
use env_logger::Env;

use clap::{Arg, App};

use crate::plugin::PluginRegistry;

mod config;
mod exec;
#[macro_use]
mod plugin;
mod flow;
mod job;
mod source;
mod message;
mod utils;

#[tokio::main]
async fn main() {
    let matches = App::new("flowrunner")
                        .version(env!("CARGO_PKG_VERSION"))
                        .about("Flow Runner helps to run a flow simply and standalone!")
                        .setting(clap::AppSettings::TrailingVarArg)
                        .setting(clap::AppSettings::AllowLeadingHyphen)
                        .arg(Arg::with_name("config")
                            .short("c")
                            .long("config")
                            .value_name("FILE")
                            .default_value(".flowrunner.yaml")
                            .help("Sets a custom config file")
                            .takes_value(true))
                        .arg(Arg::with_name("plugin-dir")
                            .short("p")
                            .long("--plugin-dir")
                            .takes_value(true)
                            .help("Module directory"))
                        .arg(Arg::with_name("flow-dir")
                            .short("w")
                            .long("--flow-dir")
                            .takes_value(true)
                            .help("Flow directory"))
                        .subcommand(
                            App::new("exec")
                                .about("Execute a flow given by a file")
                                .arg(Arg::with_name("flow-file")
                                    .long("--flow-file")
                                    .short("f")
                                    .takes_value(true)
                                    .help("Name of the flow file to execute")))
                        .get_matches();

    env_logger::init();

    // Gets a value for config if supplied by user, or defaults to ".mgr.yaml"
    let config_file = matches.value_of("config");
    let mut config = config::new(config_file.unwrap_or(".semrel-rs.yaml")).unwrap();
    info!("--- Configuration ---");
    info!("File: {:?}", config_file);
    info!("Content: {:?}", config);

    let plugin_dir = matches.value_of("plugin-dir").unwrap_or("plugins");
    let flow_dir = matches.value_of("flow-dir").unwrap_or("flows");

    info!("--- Flags ---");
    info!("Plugin directory: {}", plugin_dir);
    info!("Flow directory: {}", flow_dir);

    config.runner.plugin_dir = plugin_dir.clone().to_owned();
    config.runner.flow_dir = flow_dir.clone().to_owned();

    info!("--- Final configuration ---");
    info!("{:?}", config);

    PluginRegistry::load_plugins(plugin_dir).await;

    match matches.subcommand() {
        ("exec", Some(exec_matches)) => {
            match exec::exec_cmd(&config, exec_matches).await {
                Ok(()) => (),
                Err(e) => { error!("{}", e.to_string()); },
            }
        },
        _ => error!("Command not found"),
    }
}
