
use std::{str::FromStr, net::SocketAddr, time::Instant};

use anyhow::Result;
use crossbeam_channel::Sender;
use serde_json::Value;
use sqlx::types::{Json, Uuid};
use tokio::runtime::Runtime;
use sha2::Digest;

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
        let pool = sqlx::postgres::PgPool::connect("postgres://localhost/postgres").await?;

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

    if req.uri().path().starts_with("/api/tx/") {
        match req.method() {
            &Method::GET => {
                let path = req.uri().path();
                match Txid::from_str(&path[8..]) {
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
                                log::warn!("Txid not found: {}", txid);
                                Ok(response)
                            }
                            Err(err) => {
                                log::error!("Internal error: {:?}", err);
                                let response = builder
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(format!("Error while retrieving tx: {:?}", err)))
                                    .unwrap();
                                stats.response_duration.observe("500", start.elapsed().as_secs_f64());
                                log::error!("Internal error when handling tx {}: {:?}", txid, err);
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
                        log::warn!("Could not parse txid `{}`: {}", &path[4..], err);
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
                log::warn!("Method not allowed: {}", req.method());
                Ok(response)
            }
        }
    } else {
        let response = builder
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Path not found."))
            .unwrap();
        stats.response_duration.observe("404", start.elapsed().as_secs_f64());
        log::warn!("Path not found: {}", req.uri().path());
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

async fn create_user(pool: &sqlx::PgPool, email: &str, password: &str) -> Result<()> {
    // Check for existing user with email
    let existing_user = sqlx::query!(
            r#"SELECT id FROM users WHERE email = $1"#,
            email
        )
        .fetch_optional(pool)
        .await?;

    if existing_user.is_some() {
        return Err(anyhow::anyhow!("Email already exists"));
    }

    let password_salt: [u8; 16] = rand::random();
    let password_hash = hash_password(password, &password_salt);

    sqlx::query!(
            r#"INSERT INTO users (email, password_hash, password_salt) VALUES ($1, $2, $3)"#,
            email,
            &password_hash as &[u8],
            &password_salt as &[u8; 16]
        )
        .execute(pool)
        .await?;

    Ok(())
}

fn hash_password(password: &str, salt: &[u8]) -> Vec<u8> {
    let mut hasher = sha2::Sha256::new();
    hasher.update(password);
    hasher.update(salt);
    hasher.finalize().as_slice().to_vec()
}

enum LoginResult {
    Success {
        user_id: i32,
        session_id: Uuid,
    },
    InvalidPassword,
    UserNotFound,
}

async fn login(pool: &sqlx::PgPool, email: &str, password: &str) -> Result<LoginResult> {
    struct User {
        id: i32,
        password_hash: Vec<u8>,
        password_salt: Vec<u8>,
    }

    let user = sqlx::query_as!(
            User,
            r#"SELECT id, password_hash, password_salt FROM users WHERE email = $1"#,
            email
        )
        .fetch_optional(pool)
        .await?;

    match user {
        None => Ok(LoginResult::UserNotFound),
        Some(user) => {

            let password_hash = hash_password(password, &user.password_salt[0..16]);

            if password_hash != user.password_hash {
                return Ok(LoginResult::InvalidPassword);
            }

            let session_id = Uuid::from_bytes(rand::random());

            sqlx::query!(
                    r#"INSERT INTO sessions (id, user_id) VALUES ($1, $2)"#,
                    session_id,
                    user.id,
                )
                .execute(pool)
                .await?;

            Ok(LoginResult::Success {
                user_id: user.id,
                session_id,
            })
        }
    }
}

async fn authenticate(pool: &sqlx::PgPool, session_id: Uuid) -> Result<i32> {
    let session = sqlx::query!(
            r#"SELECT user_id FROM sessions WHERE id = $1"#,
            session_id
        )
        .fetch_optional(pool)
        .await?;

    let session = session.ok_or_else(|| anyhow::anyhow!("Invalid session"))?;
    Ok(session.user_id)
}

async fn create_project(
    pool: &sqlx::PgPool,
    session_id: Uuid,
    name: &str,
    data: serde_json::Value,
    is_private: bool
) -> Result<i32> {
    let user_id = authenticate(pool, session_id).await?;

    let row = sqlx::query!(
            r#"INSERT INTO projects (user_id, name, data, is_private) VALUES ($1, $2, $3, $4) RETURNING id"#,
            user_id,
            name,
            data,
            is_private
        )
        .fetch_one(pool)
        .await?;

    Ok(row.id)
}
