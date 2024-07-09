use crate::httpserver::handlers::{
    current_config, rbatis_t_insert, redis_put, root, task_all, task_all_living, task_analyze,
    task_create, task_remove, task_show, task_start, task_status, task_stop,
    task_template_transfer_local2local, task_template_transfer_local2oss,
    task_template_transfer_oss2local, task_template_transfer_oss2oss, task_update,
};

use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{BoxError, Router};

use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

pub fn router_root() -> Router {
    let tracer = TraceLayer::new_for_http();
    let middleware_stack = ServiceBuilder::new()
        .layer(tracer)
        .layer(CompressionLayer::new())
        .layer(HandleErrorLayer::new(handle_timeout_error))
        .layer(tower::timeout::TimeoutLayer::new(Duration::from_secs(2)))
        .into_inner();

    // let static_files_service: MethodRouter<Body> = get_service(
    //     ServeDir::new("./dist").append_index_html_on_directories(true),
    // )
    // .handle_error(|error: std::io::Error| async move {
    //     (
    //         StatusCode::INTERNAL_SERVER_ERROR,
    //         format!("Unhandled internal error: {}", error),
    //     )
    // });

    let root = Router::new()
        // .route("/gethead", post(get_headers))
        .route("/health", get(root))
        .route("/health", post(root));

    let task_router = Router::new()
        .route("/create", post(task_create))
        .route("/update", post(task_update))
        .route("/remove", post(task_remove))
        .route("/start", post(task_start))
        .route("/stop", post(task_stop))
        .route("/status", post(task_status))
        .route("/show", post(task_show))
        .route("/analyze", post(task_analyze))
        .route("/all", post(task_all))
        .route("/all_living", post(task_all_living))
        .route(
            "/template/transfer/oss2oss",
            get(task_template_transfer_oss2oss),
        )
        .route(
            "/template/transfer/local2oss",
            get(task_template_transfer_local2oss),
        )
        .route(
            "/template/transfer/oss2local",
            get(task_template_transfer_oss2local),
        )
        .route(
            "/template/transfer/local2local",
            get(task_template_transfer_local2local),
        )
        .layer(middleware_stack.clone());

    let api = Router::new()
        .route("/v1/currentconfig", post(current_config))
        .route("/v1/redis/put", post(redis_put))
        .route("/v1/mysql/insert", post(rbatis_t_insert))
        .layer(middleware_stack.clone())
        .nest("/v1/task", task_router);

    return root.nest("/api", api);
}

async fn handle_timeout_error(err: BoxError) -> (StatusCode, String) {
    if err.is::<tower::timeout::error::Elapsed>() {
        (
            StatusCode::REQUEST_TIMEOUT,
            "Request took too long".to_string(),
        )
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unhandled internal error: {}", err),
        )
    }
}
