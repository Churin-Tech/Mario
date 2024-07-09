mod config;
mod handler_mysql;
mod handler_redis;
mod handler_root;
mod handler_task;
mod handler_task_template;

use axum::Json;
pub use config::current_config;
pub use handler_mysql::rbatis_t_insert;
pub use handler_redis::*;
pub use handler_root::root;
pub use handler_task::*;
pub use handler_task_template::*;

use crate::httpserver::module::Response;

type HandlerResult<T> = crate::httpserver::module::Result<Json<Response<T>>>;
