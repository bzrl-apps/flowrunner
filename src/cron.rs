use anyhow::{anyhow, Result};
use tokio::signal;

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

            if flow.kind == Kind::Cron {
                if flows.contains_key(&flow.name.clone()) {
                    panic!("The flow already exists: flow={}", flow.name);
                }

                info!("Registering flow...: flow={}", flow.name);
                flows.insert(flow.name.clone(), flow.clone());

                let schedule = flow.schedule.clone();
                let flow_cloned = flow.clone();

                info!("Adding new scheduled job: flow={}, schedule={}", flow.name, schedule);
                let mut job = Job::new_async(
                    schedule.as_str(),
                    //"1/4 * * * * *",
                    move |_uuid, _lock| {
                        let mut flow_cloned = flow_cloned.clone();
                        Box::pin(async move {
                            if let Err(e) = flow_cloned.run().await {
                                error!("Failed to run the flow: err={e}")
                            }

                            info!{"flow result: res={:?}", flow_cloned};
                        })
                    }
                )?;

                // Add job notifications
                job.on_start_notification_add(
                    &scheduler,
                    Box::new(move |job_id, notification_id, type_of_notification| {
                        Box::pin(async move {
                            info!("Job starts running: job_id={:?}, notif_id={:?}, notif_type={:?}", job_id, notification_id, type_of_notification);
                        })
                    })
                )?;
                job.on_done_notification_add(
                    &scheduler,
                    Box::new(|job_id, notification_id, type_of_notification| {
                        Box::pin(async move {
                            info!(
                                "Job completed: job_id={:?}, notif_id={:?}, notif_type={:?}",
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
        info!("Flow shut down done");
      })
    }));

    if let Err(e) = scheduler.start() {
        panic!("Failed to start scheduler: err={}", e);
    }

    match signal::ctrl_c().await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!("Unable to listen for shutdown signal: err={}", e);
            // we also shut down in case of error
            Err(anyhow!(e))
        },
    }

    //Ok(())
}
