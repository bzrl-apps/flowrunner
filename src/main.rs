extern crate log;
extern crate clap;

use log::{info, error};
use env_logger::Env;

use clap::{Arg, App, SubCommand};

mod config;
mod exec;
mod cmd_registry;

fn main() {
    let matches = App::new("flowrunner")
                        .version(env!("CARGO_PKG_VERSION"))
                        .about("An utility helps to make automatic semantic release!")
                        .setting(clap::AppSettings::TrailingVarArg)
                        .setting(clap::AppSettings::AllowLeadingHyphen)
                        .arg(Arg::with_name("config")
                            .short("c")
                            .long("config")
                            .value_name("FILE")
                            .default_value(".flowrunner.yaml")
                            .help("Sets a custom config file")
                            .takes_value(true))
                        .arg(Arg::with_name("verbose")
                            .short("v")
                            .multiple(true)
                            .help("Sets the level of verbosity"))
                        .arg(Arg::with_name("module-dir")
                            .short("m")
                            .long("--module-dir")
                            .help("Module directory"))
                        .arg(Arg::with_name("workflow-dir")
                            .short("w")
                            .long("--worflow-dir")
                            .help("Workflow directory"))
                        .subcommand(
                            App::new("exec")
                                .about("Execute workflows")
                                .arg(Arg::with_name("workflows")
                                    .long("workflows")
                                    .takes_value(true)
                                    .help("List of workflows to execute, separated by semi-colon (;)")))
                        .get_matches();

    let log_level;
    match matches.occurrences_of("verbose") {
        0 => log_level = "info",
        1 => log_level = "debug",
        _ => log_level = "trace",
    }

    env_logger::Builder::from_env(Env::default().default_filter_or(log_level)).init();

    // Gets a value for config if supplied by user, or defaults to ".mgr.yaml"
    let config_file = matches.value_of("config");
    let mut config = config::new(config_file.unwrap_or(".semrel-rs.yaml")).unwrap();
    info!("--- Configuration ---");
    info!("File: {:?}", config_file);
    info!("Content: {:?}", config);

    let module_dir = matches.value_of("module-dir").unwrap_or("modules").to_string();
    let workflow_dir = matches.value_of("workflow-dir").unwrap_or("workflows").to_string();

    info!("--- Flags ---");
    info!("Module directory: {}", module_dir);
    info!("Workflow directory: {}", workflow_dir);

    config.runner.module_dir = module_dir;
    config.runner.workflow_dir = workflow_dir;

    info!("--- Final configuration ---");
    info!("{:?}", config);

    match matches.subcommand() {
        ("exec", Some(exec_matches)) => {
            exec::exec_cmd(config.clone(), exec_matches);
        },
        _ => unreachable!(),
    }
}
