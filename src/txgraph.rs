use std::{fmt::Debug, net::SocketAddr, str::FromStr, time::Instant};

use anyhow::{Context, Result};
use axum::{
    extract::{FromRequestParts, Path, State},
    http::{request, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use crossbeam_channel::Sender;
use sha2::Digest;
use sqlx::types::Uuid;
use tokio::{runtime::Runtime, task::spawn_blocking};
use tower::ServiceBuilder;

use crate::{
    metrics::{self, Histogram, Metrics},
    server::Event,
};
use bitcoin::{hex::HexToArrayError, Txid};
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

        let state = AppState {
            pool,
            stats,
            server_tx,
        };

        let app = Router::new()
            .route("/tx/:txid", get(tx_get))
            .route("/user/create", post(create_user))
            .route("/user/login", post(login))
            .route("/user/logout", post(logout))
            .route("/project/create", post(create_project))
            .route("/project/:project_id", get(get_project))
            .route("/project/public/:project_id", get(get_public_project))
            .route("/projects", get(list_projects))
            .route("/project/:project_id", delete(delete_project))
            .layer(ServiceBuilder::new().map_response(cors_allow_all))
            .with_state(state);

        let api = Router::new().nest("/api", app);

        let listener = tokio::net::TcpListener::bind(options.address).await?;
        log::info!("Listening on http://{}", options.address);
        axum::serve(listener, api).await?;

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

// State

#[derive(Clone)]
struct AppState {
    pool: sqlx::PgPool,
    stats: Stats,
    server_tx: Sender<Event>,
}

// Error messages

enum AppError {
    InternalError(anyhow::Error),
    AuthenticationError(String),
    NotFound,
    CantParseTxid(HexToArrayError),
}

impl AppError {
    fn authentication_error<E: ToString>(msg: E) -> Self {
        Self::AuthenticationError(msg.to_string())
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self::InternalError(err.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status_msg = match self {
            Self::InternalError(err) => {
                log::error!("Internal error: {}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Internal error: {}", err),
                )
            }
            Self::AuthenticationError(msg) => (StatusCode::UNAUTHORIZED, msg),
            Self::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            Self::CantParseTxid(err) => (
                StatusCode::BAD_REQUEST,
                format!("Can't parse txid: {}", err),
            ),
        };
        status_msg.into_response()
    }
}

fn cors_allow_all(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    response
}

/// Byte vector wrapper to deal with BYTEA Postgres type properly.
struct Bytea(Vec<u8>);

impl From<String> for Bytea {
    fn from(s: String) -> Self {
        let s = s.trim_start_matches("\\x");
        Self(hex::decode(s).unwrap())
    }
}

struct AuthenticatedUser(i32);

#[axum::async_trait]
impl FromRequestParts<AppState> for AuthenticatedUser {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(session_id) = parts.headers.get("Session") {
            let session_id = session_id
                .to_str()
                .map_err(|e| AppError::authentication_error(format!("Can't read session: {e}")))?;
            let session_id = Uuid::parse_str(session_id)
                .map_err(|e| AppError::authentication_error(format!("Can't read session: {e}")))?;
            let user_id = authenticate(&state.pool, session_id).await?;
            Ok(Self(user_id))
        } else {
            Err(AppError::authentication_error("Missing session header"))
        }
    }
}

async fn authenticate(pool: &sqlx::PgPool, session_id: Uuid) -> Result<i32, AppError> {
    let session = sqlx::query!(r#"SELECT user_id FROM sessions WHERE id = $1"#, session_id)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("Failed to authenticate session with id {session_id}"))?;

    let session = session.ok_or_else(|| AppError::authentication_error("Session not found"))?;
    Ok(session.user_id)
}

// GET /tx/:txid

async fn tx_get(
    Path(txid): Path<String>,
    State(AppState {
        pool: _,
        server_tx,
        stats,
    }): State<AppState>,
) -> Result<Json<Transaction>, AppError> {
    let start = Instant::now();

    let (sender, receiver) = tokio::sync::oneshot::channel();

    let txid = match Txid::from_str(&txid) {
        Ok(txid) => txid,
        Err(err) => return Err(AppError::CantParseTxid(err)),
    };

    spawn_blocking(move || server_tx.send(Event::get_tx(txid, sender))).await??;

    match receiver.await? {
        Ok(Some(tx)) => {
            let elapsed = start.elapsed().as_secs_f64();
            stats.response_duration.observe("200", elapsed);
            stats
                .response_per_input_duration
                .observe("200", elapsed / tx.inputs.len() as f64);
            stats
                .response_per_output_duration
                .observe("200", elapsed / tx.outputs.len() as f64);
            Ok(Json(tx))
        }
        Ok(None) => {
            stats
                .response_duration
                .observe("404", start.elapsed().as_secs_f64());
            log::warn!("Txid not found: {}", txid);
            Err(AppError::NotFound)
        }
        Err(err) => {
            stats
                .response_duration
                .observe("500", start.elapsed().as_secs_f64());
            log::error!("Internal error when handling tx {}: {:?}", txid, err);
            Err(AppError::InternalError(err))
        }
    }
}

// POST /user/create

enum CreateUserResult {
    Success { user_id: i32, session_id: Uuid },
    EmailExists,
    InvalidEmail,
}

impl IntoResponse for CreateUserResult {
    fn into_response(self) -> Response {
        match self {
            Self::Success {
                user_id,
                session_id,
            } => Json(&serde_json::json!({
                "user_id": user_id,
                "session_id": session_id.to_string(),
            }))
            .into_response(),
            Self::EmailExists => {
                (StatusCode::CONFLICT, "Email already exists\n".to_string()).into_response()
            }
            Self::InvalidEmail => {
                (StatusCode::BAD_REQUEST, "Invalid email\n".into_response()).into_response()
            }
        }
    }
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

#[derive(Clone, Deserialize)]
struct UserPasswordInput {
    email: String,
    password: String,
}

async fn create_user(
    State(state): State<AppState>,
    Json(input): Json<UserPasswordInput>,
) -> Result<CreateUserResult, AppError> {
    if !is_valid_email(&input.email) {
        return Ok(CreateUserResult::InvalidEmail);
    }

    // Check for existing user with email
    let existing_user = sqlx::query!(r#"SELECT id FROM users WHERE email = $1"#, &input.email)
        .fetch_optional(&state.pool)
        .await
        .with_context(|| {
            format!(
                "Failed to check for existing user with email `{}`",
                input.email
            )
        })?;

    if existing_user.is_some() {
        return Ok(CreateUserResult::EmailExists);
    }

    let password_salt: [u8; 16] = rand::random();
    let password_hash = hash_password(&input.password, &password_salt);

    sqlx::query!(
        r#"INSERT INTO users (email, password_hash, password_salt) VALUES ($1, $2, $3)"#,
        input.email,
        &password_hash as &[u8],
        &password_salt as &[u8; 16]
    )
    .execute(&state.pool)
    .await
    .with_context(|| format!("Failed to create user with email `{}`", input.email))?;

    if let LoginResult::Success {
        user_id,
        session_id,
    } = login(State(state), Json(input.clone())).await?
    {
        Ok(CreateUserResult::Success {
            user_id,
            session_id,
        })
    } else {
        Err(AppError::InternalError(anyhow::anyhow!(
            "Failed to login after creating user with email `{}`",
            input.email
        )))
    }
}

fn hash_password(password: &str, salt: &[u8]) -> Vec<u8> {
    let mut hasher = sha2::Sha256::new();
    hasher.update(password);
    hasher.update(salt);
    hasher.finalize().as_slice().to_vec()
}

// POST /user/login

enum LoginResult {
    Success { user_id: i32, session_id: Uuid },
    InvalidPassword,
    EmailNotFound,
}

impl IntoResponse for LoginResult {
    fn into_response(self) -> Response {
        match self {
            Self::EmailNotFound => (
                StatusCode::UNAUTHORIZED,
                "Email does not exist\n".to_string(),
            )
                .into_response(),
            Self::InvalidPassword => {
                (StatusCode::UNAUTHORIZED, "Invalid password\n".to_string()).into_response()
            }
            Self::Success {
                user_id,
                session_id,
            } => Json(&serde_json::json!({
                "user_id": user_id,
                "session_id": session_id.to_string(),
            }))
            .into_response(),
        }
    }
}

async fn login(
    State(state): State<AppState>,
    Json(input): Json<UserPasswordInput>,
) -> Result<LoginResult, AppError> {
    struct User {
        id: i32,
        password_hash: Bytea,
        password_salt: Bytea,
    }

    let user = sqlx::query_as!(
        User,
        r#"SELECT id, password_hash, password_salt FROM users WHERE email = $1"#,
        input.email
    )
    .fetch_optional(&state.pool)
    .await
    .with_context(|| format!("Failed to log in user with email `{}`", input.email))?;

    match user {
        None => Ok(LoginResult::EmailNotFound),
        Some(user) => {
            let password_hash = hash_password(&input.password, &user.password_salt.0[0..16]);

            if password_hash != user.password_hash.0 {
                return Ok(LoginResult::InvalidPassword);
            }

            let session_id = Uuid::from_bytes(rand::random());

            sqlx::query!(
                r#"INSERT INTO sessions (id, user_id) VALUES ($1, $2)"#,
                session_id,
                user.id,
            )
            .execute(&state.pool)
            .await
            .with_context(|| format!("Failed to create session for user with id {}", user.id))?;

            Ok(LoginResult::Success {
                user_id: user.id,
                session_id,
            })
        }
    }
}

// POST /user/logout

async fn logout(
    AuthenticatedUser(user_id): AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<(), AppError> {
    sqlx::query!(r#"DELETE FROM sessions WHERE user_id = $1"#, user_id,)
        .execute(&state.pool)
        .await
        .with_context(|| format!("Failed to delete sessions with for user with id {user_id}"))?;

    Ok(())
}

// POST /project/create

#[derive(Deserialize)]
struct CreateProject {
    name: String,
    data: serde_json::Value,
    is_private: bool,
}

#[derive(Serialize)]
struct CreateProjectResult {
    project_id: i32,
}

async fn create_project(
    State(state): State<AppState>,
    AuthenticatedUser(user_id): AuthenticatedUser,
    Json(input): Json<CreateProject>,
) -> Result<Json<CreateProjectResult>, AppError> {
    let row = sqlx::query!(
            r#"INSERT INTO projects (user_id, name, data, is_private) VALUES ($1, $2, $3, $4) RETURNING id"#,
            user_id,
            input.name,
            input.data,
            input.is_private
        )
        .fetch_one(&state.pool)
        .await
        .with_context(|| format!("Failed to create project `{}` for user with id {user_id}", input.name))?;

    Ok(Json(CreateProjectResult { project_id: row.id }))
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

/// Get a project belonging to the authenticated user.
async fn get_project(
    Path(project_id): Path<i32>,
    AuthenticatedUser(user_id): AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<Project>, AppError> {
    let row = sqlx::query_as!(
        Project,
        r#"SELECT * FROM projects WHERE id = $1 AND user_id = $2"#,
        project_id,
        user_id
    )
    .fetch_optional(&state.pool)
    .await
    .with_context(|| {
        format!("Failed to get project with id {project_id} for user with id {user_id}")
    })?;

    let row = row.ok_or(AppError::NotFound)?;
    Ok(Json(row))
}

// GET /project/public/:project_id

/// Get a public project.
async fn get_public_project(
    Path(project_id): Path<i32>,
    State(state): State<AppState>,
) -> Result<Json<Project>, AppError> {
    let row = sqlx::query_as!(
        Project,
        r#"SELECT * FROM projects WHERE id = $1 AND is_private = false"#,
        project_id
    )
    .fetch_optional(&state.pool)
    .await
    .with_context(|| format!("Failed to get public project with id {project_id}"))?;

    let row = row.ok_or(AppError::NotFound)?;
    Ok(Json(row))
}

// GET /projects

#[derive(Serialize)]
struct ProjectEntry {
    id: i32,
    name: String,
}

async fn list_projects(
    AuthenticatedUser(user_id): AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<ProjectEntry>>, AppError> {
    let rows = sqlx::query_as!(
        ProjectEntry,
        r#"SELECT id, name FROM projects WHERE user_id = $1"#,
        user_id
    )
    .fetch_all(&state.pool)
    .await
    .with_context(|| format!("Failed to list projects for user with id {user_id}"))?;

    Ok(Json(rows))
}

// DELETE /project/:project_id

async fn delete_project(
    Path(project_id): Path<i32>,
    AuthenticatedUser(user_id): AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<(), AppError> {
    let row = sqlx::query!(
        r#"DELETE FROM projects WHERE id = $1 AND user_id = $2 RETURNING id"#,
        project_id,
        user_id
    )
    .fetch_optional(&state.pool)
    .await
    .with_context(|| {
        format!("Failed to delete project with id {project_id} for user with id {user_id}")
    })?;

    match row {
        Some(_) => Ok(()),
        None => Err(AppError::NotFound),
    }
}
