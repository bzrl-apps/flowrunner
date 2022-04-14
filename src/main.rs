extern crate lazy_static;
extern crate log;
extern crate clap;
extern crate json_ops;
extern crate sys_info;

use log::{info, error};

use clap::{Arg, App};

use crate::plugin::PluginRegistry;

mod config;
mod exec;
#[macro_use]
mod plugin;
mod flow;
mod job;
mod source;
mod sink;
mod message;
mod utils;
mod datastore;
mod server;
mod tera;

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
                        .subcommand(
                            App::new("server")
                                .about("Launch a flow server that only take classic flows")
                                .arg(Arg::with_name("host-addr")
                                    .default_value("127.0.0.1:3000")
                                    .long("--host-addr")
                                    .takes_value(true)
                                    .help("IP address & port")))
                        .get_matches();

    env_logger::init();

    // Gets a value for config if supplied by user, or defaults to ".mgr.yaml"
    let config_file = matches.value_of("config");
    let mut config = config::new(config_file.unwrap_or(".flowrunner.yaml")).unwrap();
    info!("--- Configuration ---");
    info!("File: {:?}", config_file);
    info!("Content: {:?}", config);

    if let Some(plugin_dir) = matches.value_of("plugin-dir") {
        config.runner.plugin_dir = <&str>::clone(&plugin_dir).to_owned();
    }

    if let Some(flow_dir) = matches.value_of("flow-dir") {
        config.runner.flow_dir = <&str>::clone(&flow_dir).to_owned();
    }

    info!("--- Flags ---");
    info!("Plugin directory: {}", config.runner.plugin_dir);
    info!("Flow directory: {}", config.runner.flow_dir);

    info!("--- Final configuration ---");
    info!("{:?}", config);

    PluginRegistry::load_plugins(config.runner.plugin_dir.as_str()).await;

    match matches.subcommand() {
        ("exec", Some(exec_matches)) => {
            match exec::exec_cmd(&config, exec_matches).await {
                Ok(()) => (),
                Err(e) => { error!("{}", e.to_string()); },
            }
        },
        ("server", Some(server_matches)) => {
            match server::server_run(&config, server_matches).await {
                Ok(()) => (),
                Err(e) => { error!("{}", e.to_string()); },
            }
        },
        _ => error!("Command not found"),
    }
}
