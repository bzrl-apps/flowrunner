use clap::ArgMatches;

use anyhow::{anyhow, Result};
use tokio::signal;
use log::{error, info};

use std::fs;
use std::collections::HashMap;

use log::*;

use tokio_cron_scheduler::{JobScheduler, Job};

use crate::config::Config;
use crate::flow::{Flow, Kind};

pub async fn cron_run(config: &Config) -> Result<()> {
    let mut scheduler = JobScheduler::new()?;
    let mut flows: HashMap<String, Flow> = HashMap::new();

    let paths = fs::read_dir(config.runner.flow_dir.clone()).unwrap_or_else(|_| panic!("Cannot read files in the directory {}", config.runner.flow_dir));
    for path in paths {
        if let Some(p) = path?.path().to_str() {
            let flow = Flow::new_from_file(p)?;

            debug!("{:#?}", flow);
            if flow.kind == Kind::Cron {
                if flows.contains_key(&flow.name.clone()) {
                    panic!("The flow {} already exists", flow.name);
                }

                info!("Registering the flow {} ...", flow.name);
                flows.insert(flow.name.clone(), flow.clone());

                let schedule = flow.schedule.clone();
                let flow_cloned = flow.clone();
                let mut job = Job::new_async(
                    schedule.as_str(),
                    //"1/4 * * * * *",
                    move |_uuid, _lock| {
                        let mut flow_cloned = flow_cloned.clone();
                        Box::pin(async move {
                            println!("{:?} I run async every 4 seconds", chrono::Utc::now());
                            let _ = flow_cloned.run().await;
                            info!{"flow result: {:#?}", flow_cloned};
                        })
                    }
                )?;
                //let mut job = Job::new_async("1/4 * * * * *", |_uuid, _l| Box::pin(async move {
                    //println!("{:?} I run async every 4 seconds", chrono::Utc::now());
                    //})).unwrap();

                // Add job notifications
                job.on_start_notification_add(
                    &scheduler,
                    Box::new(move |job_id, notification_id, type_of_notification| {
                        Box::pin(async move {
                            info!("Job {:?} ran on start notification {:?} ({:?})", job_id, notification_id, type_of_notification);
                        })
                    })
                )?;
                job.on_done_notification_add(
                    &scheduler,
                    Box::new(|job_id, notification_id, type_of_notification| {
                        Box::pin(async move {
                            info!(
                                "Job {:?} completed and ran notification {:?} ({:?})",
                                job_id, notification_id, type_of_notification
                            );
                        })
                    }),
                )?;

                scheduler.add(job)?;
            }
        }
    }

    scheduler.set_shutdown_handler(Box::new(|| {
      Box::pin(async move {
        println!("Shut down done");
      })
    }));

    scheduler.start();

    match signal::ctrl_c().await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!("Unable to listen for shutdown signal: {}", e);
            // we also shut down in case of error
            Err(anyhow!(e))
        },
    }

    //Ok(())
}
