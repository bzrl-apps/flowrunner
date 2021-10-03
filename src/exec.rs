use clap::ArgMatches;

use crate::config::Config;

pub async fn exec_cmd(config: Config, matches: &ArgMatches<'_>) {

    //let string = matches.value_of("workflows").expected("you must specify a list of workflows");
    //let wfs: Vec<&str> = string.split(";").collect();

    //for name in wfs.iter() {
            //flow::execute(name)
    //}
}
