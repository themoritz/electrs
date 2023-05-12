
use std::{str::FromStr, path::Path, net::SocketAddr};

use anyhow::Result;
use crossbeam_channel::Sender;
use tokio::runtime::Runtime;

use crate::server::Event;
use bitcoin::Txid;
use hyper::{header, Body, Method, Request, Response, StatusCode, service::{service_fn, make_service_fn}, Server};
use hyper_staticfile::Static;
use serde::{Deserialize, Serialize};

type GenericError = Box<dyn std::error::Error + Send + Sync>;

pub struct Options {
    pub dev: bool,
    pub address: SocketAddr,
    pub static_files: String,
}

pub fn main(server_tx: Sender<Event>, options: Options) -> Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let static_ = Static::new(Path::new(&options.static_files));

        let service = make_service_fn(move |_| {
            let server_tx = server_tx.clone();
            let static_ = static_.clone();
            async move {
                Ok::<_, GenericError>(service_fn(move |req| {
                    server(static_.to_owned(), server_tx.to_owned(), options.dev, req)
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
    static_: Static,
    server_tx: Sender<Event>,
    dev: bool,
    req: Request<Body>,
) -> Result<Response<Body>, std::io::Error> {
    let builder = if dev {
        Response::builder().header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
    } else {
        Response::builder()
    };

    if req.uri().path().starts_with("/tx/") {
        match req.method() {
            &Method::GET => {
                let path = req.uri().path();
                match Txid::from_str(&path[4..]) {
                    Ok(txid) => {
                        let (sender, receiver) = crossbeam_channel::bounded(0);
                        server_tx.send(Event::get_tx(txid, sender)).unwrap();
                        match receiver.recv().unwrap() {
                            Ok(tx) => {
                                let json = serde_json::to_string(&tx).unwrap();
                                let response = builder
                                    .header(header::CONTENT_TYPE, "application/json")
                                    .body(Body::from(json))
                                    .unwrap();

                                Ok(response)
                            }
                            Err(err) => {
                                let response = builder
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Body::from(format!("Tx not found: {:?}", err)))
                                    .unwrap();
                                Ok(response)
                            }
                        }
                    },
                    Err(err) => {
                        let response = builder
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(format!("Could not parse txid: {}", err)))
                            .unwrap();
                        Ok(response)
                    }
                }
            }
            _ => {
                let response = builder
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(Body::empty())
                    .unwrap();
                Ok(response)
            }
        }
    } else {
        static_.serve(req).await
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
