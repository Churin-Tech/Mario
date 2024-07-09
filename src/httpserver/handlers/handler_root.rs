use crate::httpserver::module::Response;
use axum::Json;
use serde_json::{json, Value};

use super::HandlerResult;

pub async fn root() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"health":"ok"}))))
}
