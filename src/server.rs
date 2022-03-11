use clap::ArgMatches;

use anyhow::{anyhow, Result};
use futures::AsyncReadExt;
use tokio::signal;
use log::*;

use std::fs;
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::{Arc, RwLock};

use axum::Router;
use axum::routing::*;
use axum::extract::{Path, Json, Extension};
use axum::response::IntoResponse;
use axum::http::StatusCode;

use crate::config::Config;
use crate::flow::{Flow, Kind};

#[derive(Default)]
struct State {
    flows: HashMap<String, Flow>,
}

type SharedState = Arc<RwLock<State>>;

pub async fn server_run(config: &Config, matches: &ArgMatches<'_>) -> Result<()> {
    let mut flows: HashMap<String, Flow> = HashMap::new();

    let host_addr = match matches.value_of("host-addr") {
        Some(h) => h,
        None => return Err(anyhow!("You must specify the host addr (--host-addr)")),
    };

    let paths = fs::read_dir(config.runner.flow_dir.clone()).expect(format!("Cannot read files in the directory {}", config.runner.flow_dir).as_str());
    for path in paths {
        if let Some(p) = path?.path().to_str() {
            let flow = Flow::new_from_file(p)?;

            debug!("{:#?}", flow);
            if flow.kind == Kind::Action {
                if flows.contains_key(&flow.name.clone()) {
                    panic!("The flow {} already exists", flow.name.clone());
                }

                info!("Registering the flow {} ...", flow.name.clone());
                flows.insert(flow.name.clone(), flow);
            }
        }
    }

    let state = State {flows};
    let shared_state: SharedState = Arc::new(RwLock::new(state));
    // Build our application with a route
    let app = Router::new()
        .route("/flows/:flow", post(handler))
        .layer(Extension(shared_state));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = host_addr.parse::<SocketAddrV4>().map_err(|e| { error!("{e}"); e })?;

    info!("Listening on {:?}", addr);
    if let Err(e) = axum::Server::bind(&SocketAddr::V4(addr))
        .serve(app.into_make_service())
        .await {
        error!("{e}");
    }

    Ok(())
}

async fn handler(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Path(flow): Path<String>,
    Extension(state): Extension<SharedState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {

    let mut flows = state.write().unwrap().flows.clone();
    if let Some(f) = flows.get_mut(&flow) {
        if let Err(e) = f.run().await {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("{{\"error\": {}}}", e)));
        }

        return Ok(Json(f.jobs.clone()));
    }

    Err((StatusCode::NOT_FOUND, format!("{{\"error\": flow {} not found}}", flow)))
}
