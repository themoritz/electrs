use std::collections::HashMap;

use axum::{
    extract::FromRequestParts,
    http::{request, HeaderValue},
};
use bitcoin::Txid;
use lazy_static::lazy_static;

use crate::txgraph::AppError;

lazy_static! {
    static ref API_KEYS: HashMap<String, String> = {
        include_str!("api_auth/keys.txt")
            .lines()
            .map(|line| {
                let mut parts = line.split_whitespace();
                let key = parts.next().unwrap().to_string();
                let client = parts.next().unwrap().to_string();
                (key, client)
            })
            .collect()
    };
}

pub enum ApiAuth {
    BearerToken { token: String, ip: Option<String> },
    ClientSignature { signature: Vec<u8> },
}

impl ApiAuth {
    /// Returns the name of the client that made the request.
    pub fn authenticate(&self, txid: &Txid) -> Result<String, AppError> {
        match self {
            ApiAuth::BearerToken { token, .. } => {
                match API_KEYS.get(token) {
                    Some(client) => Ok(client.clone()),
                    None => Err(AppError::authentication_error("Invalid token")),
                }
            },
            ApiAuth::ClientSignature { signature } => {
                Err(AppError::authentication_error("Client signature not implemented"))
            },
        }
    }

    pub fn ip(&self) -> Option<String> {
        match self {
            ApiAuth::BearerToken { ip, .. } => ip.to_owned(),
            ApiAuth::ClientSignature { .. } => None,
        }
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for ApiAuth {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        if let Some(sig) = parts.headers.get("X-Request-Signature") {
            let sig_bytes = base64::decode(sig.as_bytes()).map_err(|e| {
                AppError::authentication_error(format!("Can't decode signature: {e}"))
            })?;
            Ok(Self::ClientSignature {
                signature: sig_bytes,
            })
        } else if let Some(token) = parts.headers.get("Authorization") {
            let ip = match parts.headers.get("X-Real-IP") {
                None => None,
                Some(ip) => Some(header_value_to_string(ip)?),
            };
            if let Some(token) = header_value_to_string(token)?.strip_prefix("Bearer ") {
                Ok(Self::BearerToken {
                    token: token.to_string(),
                    ip,
                })
            } else {
                Err(AppError::authentication_error("Expected bearer token"))
            }
        } else {
            Err(AppError::authentication_error(
                "Missing authentication header",
            ))
        }
    }
}

fn header_value_to_string(header: &HeaderValue) -> Result<String, AppError> {
    match header.to_str() {
        Ok(str) => Ok(str.to_string()),
        Err(_) => Err(AppError::authentication_error("Invalid header value")),
    }
}
