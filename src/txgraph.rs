
use std::{str::FromStr, net::SocketAddr, time::Instant};

use anyhow::Result;
use crossbeam_channel::Sender;
use tokio::runtime::Runtime;

use crate::{server::Event, metrics::{Histogram, self, Metrics}};
use bitcoin::Txid;
use hyper::{header, Body, Method, Request, Response, StatusCode, service::{service_fn, make_service_fn}, Server};
use serde::{Deserialize, Serialize};

type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
pub struct Stats {
    response_duration: Histogram,
    response_per_input_duration: Histogram,
    response_per_output_duration: Histogram,
}

impl Stats {
    fn new(metrics: &Metrics) -> Self {
        Self {
            response_duration: metrics.histogram_vec(
                "response_duration",
                "Tx response duration (in seconds)",
                "code",
                metrics::default_duration_buckets(),
            ),
            response_per_input_duration: metrics.histogram_vec(
                "response_per_input_duration",
                "Tx response duration per input (in seconds)",
                "code", // 200 only
                metrics::default_duration_buckets(),
            ),
            response_per_output_duration: metrics.histogram_vec(
                "response_per_output_duration",
                "Tx response duration per output (in seconds)",
                "code", // 200 only
                metrics::default_duration_buckets(),
            ),
        }
    }
}

pub struct Options {
    pub dev: bool,
    pub address: SocketAddr,
}

pub fn main(server_tx: Sender<Event>, metrics: &Metrics, options: Options) -> Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let stats = Stats::new(metrics);
        let service = make_service_fn(move |_| {
            let server_tx = server_tx.clone();
            let stats = stats.clone();
            async move {
                Ok::<_, GenericError>(service_fn(move |req| {
                    server(server_tx.to_owned(), stats.to_owned(), options.dev, req)
                }))
            }
        });

        let server = Server::bind(&options.address)
            .serve(service);
        log::info!("Listening on http://{}", options.address);

        server.await?;

        Ok(())
    })
}

pub async fn server(
    server_tx: Sender<Event>,
    stats: Stats,
    dev: bool,
    req: Request<Body>,
) -> Result<Response<Body>, std::io::Error> {
    let builder = if dev {
        Response::builder().header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
    } else {
        Response::builder()
    };

    let start = Instant::now();

    if req.uri().path().starts_with("/tx/") {
        match req.method() {
            &Method::GET => {
                let path = req.uri().path();
                match Txid::from_str(&path[4..]) {
                    Ok(txid) => {
                        let (sender, receiver) = crossbeam_channel::bounded(0);
                        server_tx.send(Event::get_tx(txid, sender)).unwrap();
                        match receiver.recv().unwrap() {
                            Ok(Some(tx)) => {
                                let json = serde_json::to_string(&tx).unwrap();
                                let response = builder
                                    .header(header::CONTENT_TYPE, "application/json")
                                    .body(Body::from(json))
                                    .unwrap();
                                let elapsed = start.elapsed().as_secs_f64();
                                stats.response_duration.observe("200", elapsed);
                                stats.response_per_input_duration.observe("200", elapsed / tx.inputs.len() as f64);
                                stats.response_per_output_duration.observe("200", elapsed / tx.outputs.len() as f64);
                                Ok(response)
                            }
                            Ok(None) => {
                                let response = builder
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Body::from(format!("Txid not found: {}", txid)))
                                    .unwrap();
                                stats.response_duration.observe("404", start.elapsed().as_secs_f64());
                                Ok(response)
                            }
                            Err(err) => {
                                log::error!("Internal error: {:?}", err);
                                let response = builder
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(format!("Error while retrieving tx: {:?}", err)))
                                    .unwrap();
                                stats.response_duration.observe("500", start.elapsed().as_secs_f64());
                                Ok(response)
                            }
                        }
                    },
                    Err(err) => {
                        let response = builder
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(format!("Could not parse txid: {}", err)))
                            .unwrap();
                        stats.response_duration.observe("400", start.elapsed().as_secs_f64());
                        Ok(response)
                    }
                }
            }
            _ => {
                let response = builder
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(Body::empty())
                    .unwrap();
                stats.response_duration.observe("405", start.elapsed().as_secs_f64());
                Ok(response)
            }
        }
    } else {
        let response = builder
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Path not found."))
            .unwrap();
        stats.response_duration.observe("404", start.elapsed().as_secs_f64());
        Ok(response)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub timestamp: u32,
    pub block_height: u32,
    pub txid: String,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Input {
    pub txid: Txid,
    pub vout: u32,
    pub value: u64,
    pub address: String,
    pub address_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    pub spending_txid: Option<Txid>,
    pub value: u64,
    pub address: String,
    pub address_type: String,
}
