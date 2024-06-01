use std::{convert::Infallible, net::SocketAddr, time::Instant};

use anyhow::Result;
use crossbeam_channel::Sender;
use sha2::Digest;
use sqlx::types::{chrono, Uuid};
use tokio::runtime::Runtime;
use warp::{filters::reply::WithHeader, reply::Reply, Filter};

use crate::{
    metrics::{self, Histogram, Metrics},
    server::Event,
};
use bitcoin::Txid;
use serde::{Deserialize, Serialize};

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

        let routes = tx_get_filter(server_tx, stats)
            .or(create_user_filter(pool.clone()))
            .or(login_filter(pool.clone()))
            .or(create_project_filter(pool.clone()))
            .or(get_project_filter(pool.clone()))
            .or(get_public_project_filter(pool.clone()));

        let api = warp::path("api")
            .and(routes)
            .recover(handle_rejection)
            .with(allow_all_origin());

        log::info!("Listening on http://{}", options.address);
        warp::serve(api).run(options.address).await;

        Ok(())
    })
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

// Error messages

#[derive(Debug)]
struct InternalError(anyhow::Error);

#[derive(Debug)]
struct DbError(sqlx::Error);

fn from_sqlx_error(err: sqlx::Error) -> warp::Rejection {
    warp::reject::custom(DbError(err))
}

#[derive(Debug)]
struct SessionNotFound;

impl warp::reject::Reject for InternalError {}
impl warp::reject::Reject for DbError {}
impl warp::reject::Reject for SessionNotFound {}

async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, warp::Rejection> {
    if let Some(err) = err.find::<InternalError>() {
        log::error!("Internal error: {:?}", err.0);
        Ok(warp::reply::with_status(
            warp::reply().into_response(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))
    } else if let Some(err) = err.find::<DbError>() {
        log::error!("Database error: {:?}", err.0);
        Ok(warp::reply::with_status(
            warp::reply().into_response(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))
    } else if err.find::<SessionNotFound>().is_some() {
        Ok(warp::reply::with_status(
            "Session not found\n".into_response(),
            warp::http::StatusCode::UNAUTHORIZED,
        ))
    } else {
        Err(err)
    }
}

// Filter and handler helpers

struct Bytea(Vec<u8>);

impl From<String> for Bytea {
    fn from(s: String) -> Self {
        let s = s.trim_start_matches("\\x");
        Self(hex::decode(s).unwrap())
    }
}

fn session() -> impl warp::Filter<Extract = (sqlx::types::Uuid,), Error = warp::Rejection> + Clone {
    warp::header::<Uuid>("Session")
}

fn with_db(
    pool: sqlx::PgPool,
) -> impl warp::Filter<Extract = (sqlx::PgPool,), Error = Infallible> + Clone {
    warp::any().map(move || pool.clone())
}

fn allow_all_origin() -> WithHeader {
    warp::reply::with::header("Access-Control-Allow-Origin", "*")
}

async fn authenticate(pool: &sqlx::PgPool, session_id: Uuid) -> Result<i32, warp::Rejection> {
    let session = sqlx::query!(r#"SELECT user_id FROM sessions WHERE id = $1"#, session_id)
        .fetch_optional(pool)
        .await
        .map_err(from_sqlx_error)?;

    let session = session.ok_or_else(|| warp::reject::custom(SessionNotFound))?;
    Ok(session.user_id)
}

// GET /tx/:txid

fn tx_get_filter(
    server_tx: Sender<Event>,
    stats: Stats,
) -> impl warp::Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("tx" / Txid)
        .and(warp::get())
        .and(warp::any().map(move || server_tx.clone()))
        .and(warp::any().map(move || stats.clone()))
        .and_then(tx_get)
}

async fn tx_get(
    txid: Txid,
    server_tx: Sender<Event>,
    stats: Stats,
) -> Result<impl warp::Reply, warp::Rejection> {
    let start = Instant::now();

    let (sender, receiver) = crossbeam_channel::bounded(0);
    server_tx.send(Event::get_tx(txid, sender)).unwrap();
    match receiver.recv().unwrap() {
        // TODO: make async
        Ok(Some(tx)) => {
            let elapsed = start.elapsed().as_secs_f64();
            stats.response_duration.observe("200", elapsed);
            stats
                .response_per_input_duration
                .observe("200", elapsed / tx.inputs.len() as f64);
            stats
                .response_per_output_duration
                .observe("200", elapsed / tx.outputs.len() as f64);
            Ok(warp::reply::json(&tx))
        }
        Ok(None) => {
            stats
                .response_duration
                .observe("404", start.elapsed().as_secs_f64());
            log::warn!("Txid not found: {}", txid);
            Err(warp::reject::not_found())
        }
        Err(err) => {
            stats
                .response_duration
                .observe("500", start.elapsed().as_secs_f64());
            log::error!("Internal error when handling tx {}: {:?}", txid, err);
            Err(warp::reject::custom(InternalError(err)))
        }
    }
}

// POST /user/create

enum CreateUserResult {
    Success {
        user_id: i32,
        session_id: Uuid,
    },
    EmailExists,
    InvalidEmail,
}

impl warp::Reply for CreateUserResult {
    fn into_response(self) -> warp::reply::Response {
        match self {
            Self::Success { user_id, session_id } => warp::reply::json(&serde_json::json!({
                "user_id": user_id,
                "session_id": session_id.to_string(),
            }))
            .into_response(),
            Self::EmailExists => warp::reply::with_status(
                "Email already exists\n".into_response(),
                warp::http::StatusCode::CONFLICT,
            )
            .into_response(),
            Self::InvalidEmail => warp::reply::with_status(
                "Invalid email\n".into_response(),
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response(),
        }
    }
}

fn create_user_filter(
    pool: sqlx::PgPool,
) -> impl warp::Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    #[derive(Deserialize)]
    struct CreateUser {
        email: String,
        password: String,
    }

    warp::path!("user" / "create")
        .and(warp::post())
        .and(with_db(pool))
        .and(warp::body::json())
        .and_then(|pool, input: CreateUser| create_user(pool, input.email, input.password))
}

lazy_static::lazy_static! {
    static ref EMAIL_REGEX: regex::Regex = regex::Regex::new(
        r"(?x)
            ^                          # start of the string
            [a-zA-Z0-9_.+-]+           # username: alphanumeric, dots, underscores, pluses, and hyphens
            @                          # @ symbol
            ([a-zA-Z0-9-]+\.)+         # domain name: alphanumeric and hyphens
            [a-zA-Z]{2,63}             # top-level domain: 2 to 63 alphanumeric characters
            $                          # end of the string
        "
    ).unwrap();
}

fn is_valid_email(email: &str) -> bool {
    EMAIL_REGEX.is_match(email)
}

async fn create_user(
    pool: sqlx::PgPool,
    email: String,
    password: String,
) -> Result<CreateUserResult, warp::Rejection> {
    if !is_valid_email(&email) {
        return Ok(CreateUserResult::InvalidEmail);
    }

    // Check for existing user with email
    let existing_user = sqlx::query!(r#"SELECT id FROM users WHERE email = $1"#, email)
        .fetch_optional(&pool)
        .await
        .map_err(from_sqlx_error)?;

    if existing_user.is_some() {
        return Ok(CreateUserResult::EmailExists);
    }

    let password_salt: [u8; 16] = rand::random();
    let password_hash = hash_password(&password, &password_salt);

    sqlx::query!(
        r#"INSERT INTO users (email, password_hash, password_salt) VALUES ($1, $2, $3)"#,
        email,
        &password_hash as &[u8],
        &password_salt as &[u8; 16]
    )
    .execute(&pool)
    .await
    .map_err(from_sqlx_error)?;

    if let LoginResult::Success { user_id, session_id } = login(pool.clone(), email, password).await? {
        Ok(CreateUserResult::Success { user_id, session_id })
    } else {
        Err(warp::reject::custom(InternalError(anyhow::anyhow!("Failed to login after creating user"))))
    }
}

fn hash_password(password: &str, salt: &[u8]) -> Vec<u8> {
    let mut hasher = sha2::Sha256::new();
    hasher.update(password);
    hasher.update(salt);
    hasher.finalize().as_slice().to_vec()
}

// /user/login

fn login_filter(
    pool: sqlx::PgPool,
) -> impl warp::Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    #[derive(Deserialize)]
    struct Login {
        email: String,
        password: String,
    }

    warp::path!("user" / "login")
        .and(warp::post())
        .and(with_db(pool))
        .and(warp::body::json())
        .and_then(|pool, input: Login| login(pool, input.email, input.password))
}

enum LoginResult {
    Success { user_id: i32, session_id: Uuid },
    InvalidPassword,
    EmailNotFound,
}

impl warp::Reply for LoginResult {
    fn into_response(self) -> warp::reply::Response {
        match self {
            Self::EmailNotFound => warp::reply::with_status(
                "Email does not exist\n".into_response(),
                warp::http::StatusCode::UNAUTHORIZED,
            )
            .into_response(),
            Self::InvalidPassword => warp::reply::with_status(
                "Invalid password\n".into_response(),
                warp::http::StatusCode::UNAUTHORIZED,
            )
            .into_response(),
            Self::Success {
                user_id,
                session_id,
            } => warp::reply::json(&serde_json::json!({
                "user_id": user_id,
                "session_id": session_id.to_string(),
            }))
            .into_response(),
        }
    }
}

async fn login(
    pool: sqlx::PgPool,
    email: String,
    password: String,
) -> Result<LoginResult, warp::Rejection> {
    struct User {
        id: i32,
        password_hash: Bytea,
        password_salt: Bytea,
    }

    let user = sqlx::query_as!(
        User,
        r#"SELECT id, password_hash, password_salt FROM users WHERE email = $1"#,
        email
    )
    .fetch_optional(&pool)
    .await
    .map_err(from_sqlx_error)?;

    match user {
        None => Ok(LoginResult::EmailNotFound),
        Some(user) => {
            let password_hash = hash_password(&password, &user.password_salt.0[0..16]);

            if password_hash != user.password_hash.0 {
                return Ok(LoginResult::InvalidPassword);
            }

            let session_id = Uuid::from_bytes(rand::random());

            sqlx::query!(
                r#"INSERT INTO sessions (id, user_id) VALUES ($1, $2)"#,
                session_id,
                user.id,
            )
            .execute(&pool)
            .await
            .map_err(from_sqlx_error)?;

            Ok(LoginResult::Success {
                user_id: user.id,
                session_id,
            })
        }
    }
}

// POST /project/create

fn create_project_filter(
    pool: sqlx::PgPool,
) -> impl warp::Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    #[derive(Deserialize)]
    struct CreateProject {
        name: String,
        data: serde_json::Value,
        is_private: bool,
    }

    warp::path!("project" / "create")
        .and(warp::post())
        .and(session())
        .and(with_db(pool))
        .and(warp::body::json())
        .and_then(|session_id, pool, input: CreateProject| create_project(pool, session_id, input.name, input.data, input.is_private))
}

async fn create_project(
    pool: sqlx::PgPool,
    session_id: Uuid,
    name: String,
    data: serde_json::Value,
    is_private: bool,
) -> Result<impl warp::Reply, warp::Rejection> {
    let user_id = authenticate(&pool, session_id).await?;

    let row = sqlx::query!(
            r#"INSERT INTO projects (user_id, name, data, is_private) VALUES ($1, $2, $3, $4) RETURNING id"#,
            user_id,
            name,
            data,
            is_private
        )
        .fetch_one(&pool)
        .await
        .map_err(from_sqlx_error)?;

    Ok(warp::reply::json(&serde_json::json!({ "project_id": row.id })))
}

// GET /project/:project_id

#[derive(Serialize)]
struct Project {
    id: i32,
    user_id: i32,
    name: String,
    data: serde_json::Value,
    is_private: bool,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl Reply for Project {
    fn into_response(self) -> warp::reply::Response {
        warp::reply::json(&self).into_response()
    }
}

fn get_project_filter(
    pool: sqlx::PgPool,
) -> impl warp::Filter<Extract = (Project,), Error = warp::Rejection> + Clone {
    warp::path!("project" / i32)
        .and(warp::get())
        .and(session())
        .and(with_db(pool))
        .and_then(get_project)
}

/// Get a project belonging to the authenticated user.
async fn get_project(
    project_id: i32,
    session_id: Uuid,
    pool: sqlx::PgPool,
) -> Result<Project, warp::Rejection> {
    let user_id = authenticate(&pool, session_id).await?;

    let row = sqlx::query_as!(
        Project,
        r#"SELECT * FROM projects WHERE id = $1 AND user_id = $2"#,
        project_id,
        user_id
    )
    .fetch_optional(&pool)
    .await
    .map_err(from_sqlx_error)?;

    let row = row.ok_or_else(warp::reject::not_found)?;
    Ok(row)
}

// GET /project/public/:project_id

fn get_public_project_filter(
    pool: sqlx::PgPool,
) -> impl warp::Filter<Extract = (Project,), Error = warp::Rejection> + Clone {
    warp::path!("project" / "public" / i32)
        .and(warp::get())
        .and(with_db(pool))
        .and_then(get_public_project)
}

/// Get a public project.
async fn get_public_project(project_id: i32, pool: sqlx::PgPool) -> Result<Project, warp::Rejection> {
    let row = sqlx::query_as!(
        Project,
        r#"SELECT * FROM projects WHERE id = $1 AND is_private = false"#,
        project_id
    )
    .fetch_optional(&pool)
    .await
    .map_err(from_sqlx_error)?;

    let row = row.ok_or_else(warp::reject::not_found)?;
    Ok(row)
}
