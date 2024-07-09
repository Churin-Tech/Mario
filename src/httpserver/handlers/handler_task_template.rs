use super::HandlerResult;
use crate::{
    httpserver::{
        exception::{AppError, AppErrorType},
        module::Response,
        service::service_task_template::{
            service_task_template_transfer_local2local, service_task_template_transfer_local2oss,
            service_task_template_transfer_oss2local, service_task_template_transfer_oss2oss,
        },
    },
    tasks::Task,
};
use axum::Json;

use serde_json::{json, Value};

pub async fn task_template_transfer_oss2oss() -> HandlerResult<Task> {
    match service_task_template_transfer_oss2oss() {
        Ok(task) => Ok(Json(Response::ok(task))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_template_transfer_local2oss() -> HandlerResult<Task> {
    match service_task_template_transfer_local2oss() {
        Ok(task) => Ok(Json(Response::ok(task))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_template_transfer_oss2local() -> HandlerResult<Task> {
    match service_task_template_transfer_oss2local() {
        Ok(task) => Ok(Json(Response::ok(task))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_template_transfer_local2local() -> HandlerResult<Task> {
    match service_task_template_transfer_local2local() {
        Ok(task) => Ok(Json(Response::ok(task))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}
