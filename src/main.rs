use logger::{init_log, tracing_init};
mod cmd;
mod commons;
mod configure;
mod httpserver;
mod logger;
mod resources;
mod s3;
mod tasks;

fn main() {
    // init_log();
    tracing_init();
    cmd::run_app();
}
