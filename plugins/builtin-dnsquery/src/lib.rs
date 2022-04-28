extern crate flowrunner;
use flowrunner::plugin::{Plugin, PluginExecResult, Status};
use flowrunner::message::Message as FlowMessage;
use flowrunner::return_plugin_exec_result_err;
use flowrunner::datastore::store::BoxStore;
use flowrunner::utils::*;

extern crate json_ops;
use json_ops::JsonOps;

//use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};

use anyhow::{anyhow, Result};

//use tokio::sync::*;
use async_channel::{Sender, Receiver};
use tokio::runtime::Runtime;
use tokio::net::UdpSocket;
use async_trait::async_trait;

use log::*;
//use tracing::*;

use std::net::SocketAddr;
use std::str::FromStr;
use trust_dns_client::{
    udp::UdpClientStream,
    client::{AsyncClient, ClientHandle},
    rr::{Name, DNSClass, RecordType, RData, rdata},
};

// Our plugin implementation
#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct DnsQuery {
    nameserver: String,
    queries: Vec<Query>,
}

#[derive(Default, Debug ,Serialize, Deserialize, Clone, PartialEq)]
struct Query {
    name: String,
    rtype: String,
    #[serde(default)]
    rdata: Map<String, Value>,
    #[serde(default)]
    result: String,
    #[serde(default)]
    status: bool
}

macro_rules! query_result {
    ($qry:expr, $qry_res:expr, $result:expr) => {
        match serde_json::to_value($qry) {
            Ok(v) => $qry_res.push(v),
            Err(e) => return_plugin_exec_result_err!($result, e.to_string()),
        }
    }
}

#[async_trait]
impl Plugin for DnsQuery {
    fn get_name(&self) -> String {
        env!("CARGO_PKG_NAME").to_string()
    }

    fn get_version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }

    fn get_description(&self) -> String {
        env!("CARGO_PKG_DESCRIPTION").to_string()
    }

    fn get_params(&self) -> Map<String, Value> {
        let params: Map<String, Value> = Map::new();

        params
    }

    fn validate_params(&mut self, params: Map<String, Value>) -> Result<()> {
        let jops_params = JsonOps::new(Value::Object(params));
        let mut default = DnsQuery::default();

        match jops_params.get_value_e::<String>("nameserver") {
            Ok(v) => default.nameserver = v,
            Err(_) => { return Err(anyhow!("nameserver can not be empty. It must be specified with the format: ip:port.")); },
        };

        match jops_params.get_value_e::<Vec<Query>>("queries") {
            Ok(v) => default.queries = v,
            Err(_) => { return Err(anyhow!("queries can not be empty.")); },
        };

        *self = default;

        Ok(())
    }

    fn set_datastore(&mut self, _datastore: Option<BoxStore>) {}


    /// Apply operation per operation on target in the order. The result of previous operation will
    /// be used for the next operation.
    async fn func(&self, _sender: Option<String>, _tx: &Vec<Sender<FlowMessage>>, _rx: &Vec<Receiver<FlowMessage>>) -> PluginExecResult {
        let _ =  env_logger::try_init();

        let mut result = PluginExecResult::default();

        let rt = Runtime::new().unwrap();
        //let _guard = rt.enter();

        // Initialize tracing
        //tracing_subscriber::fmt::init();

        // Trust DNS initialization
        info!("Initializing Trust DNS client...");
        let ns: SocketAddr = match self.nameserver.parse() {
            Ok(s) => s,
            Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
        };

        rt.block_on(async {
            let stream = UdpClientStream::<UdpSocket>::new(ns);
            // Await the connection to be established
            let (mut client, server) = match AsyncClient::connect(stream).await {
                Ok((c, s)) => (c, s),
                Err(e) => return_plugin_exec_result_err!(result, e.to_string()),
            };

            // make sure to run the background task
            debug!("Spawning server in the background...");
            rt.spawn(server);

            let mut q_results: Vec<Value> = Vec::new();

            for q in self.queries.iter() {
                let mut q_res = q.clone();

                let name = match Name::from_str(&q.name) {
                    Ok(v) => v,
                    Err(e) => {
                        q_res.result = e.to_string();
                        q_res.status = false;

                        query_result!(q_res, q_results, result);

                        continue;
                    }
                };

                let rtype = match RecordType::from_str(&q.rtype) {
                    Ok(v) => v,
                    Err(e) => {
                        q_res.result = e.to_string();
                        q_res.status = false;

                        query_result!(q_res, q_results, result);

                        continue;
                    }
                };

                debug!("Querying DNS: name={name:?}, rtype={rtype:?}");

                let query = client.query(
                    name,
                    DNSClass::IN,
                    rtype);

                // wait for its response
                let response = match query.await {
                    Ok(v) => v,
                    Err(e) => {
                        q_res.result = e.to_string();
                        q_res.status = false;

                        error!("error to query dns: {e}");

                        query_result!(q_res, q_results, result);

                        continue;
                    }
                };

                debug!("DNS query response: {response:?}");

                q_res.status = false;
                q_res.result = "none".to_string();

                for a in response.answers().iter() {
                    if let Some(rd) = a.data() {
                        (q_res.status, q_res.result) = match new_rdata(rtype, q.rdata.clone()) {
                            Ok(v) => {
                                if *rd == v {
                                    (true, rd.to_string())
                                } else {
                                    (false, rd.to_string())
                                }
                            },
                            Err(e) => {
                                error!("error to create a new rdata from input: {e}");
                                (false, e.to_string())
                            }
                        };

                        if q_res.status { break; }
                    }
                }

                query_result!(q_res, q_results, result);
            }

            result.status = Status::Ok;
            result.output.insert("result".to_string(), Value::Array(q_results));

            result
        })
    }
}

fn new_rdata(rtype: RecordType, rdata: Map<String, Value>) -> Result<RData> {
    match rtype {
        RecordType::A => {
            let name = rdata.get("name")
                .ok_or(anyhow!("name must be specified in rdata for record type A"))?
                .as_str()
                .ok_or(anyhow!("cannot convert name in rdata to string"))?;

            Ok(RData::A(name.parse()?))
        },
        RecordType::CNAME => {
            let name = rdata.get("name")
                .ok_or(anyhow!("name must be specified in rdata for record type CNAME"))?
                .as_str()
                .ok_or(anyhow!("cannot convert name in rdata to string"))?;

            Ok(RData::CNAME(name.parse()?))
        },
        RecordType::PTR => {
            let name = rdata.get("name")
                .ok_or(anyhow!("name must be specified in rdata for record type PTR"))?
                .as_str()
                .ok_or(anyhow!("cannot convert name in rdata to string"))?;

            Ok(RData::PTR(name.parse()?))
        },
        RecordType::NS => {
            let name = rdata.get("name")
                .ok_or(anyhow!("name must be specified in rdata for record type NS"))?
                .as_str()
                .ok_or(anyhow!("cannot convert name in rdata to string"))?;

            Ok(RData::NS(name.parse()?))
        },
        RecordType::MX => {
            let exchange = rdata.get("exchange")
                .ok_or(anyhow!("exchange must be specified in rdata for record type MX"))?
                .as_str()
                .ok_or(anyhow!("cannot convert exchange in rdata to string"))?;

            let preference = rdata.get("preference")
                .ok_or(anyhow!("exchange must be specified in rdata for record type MX"))?
                .as_u64()
                .ok_or(anyhow!("cannot convert exchange in rdata to u16"))?;

            Ok(RData::MX(rdata::MX::new(preference as u16, Name::from_str(exchange)?)))
        },
        RecordType::TXT => {
            let txt_data = rdata.get("txt_data")
                .ok_or(anyhow!("txt_data must be specified in rdata for record type TXT"))?
                .as_array()
                .ok_or(anyhow!("cannot convert txt_data in rdata to Vec<String>"))?
                .iter()
                .map(|e| e.as_str().unwrap_or_default().to_string())
                .collect();

            Ok(RData::TXT(rdata::TXT::new(txt_data)))
        }
        _ => Err(anyhow!(format!("Record type {} not supported", rtype.to_string()))),
    }
}

#[no_mangle]
pub fn get_plugin() -> *mut (dyn Plugin + Send + Sync) {
    debug!("Plugin DnsQuery loaded!");

    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(DnsQuery::default()))
}
