mod service_mysql;
mod service_redis;
pub(crate) mod service_task;
pub(crate) mod service_task_template;

pub use service_mysql::insert_rbatis_t;
pub use service_redis::put;
// pub use service_task_template::*;
