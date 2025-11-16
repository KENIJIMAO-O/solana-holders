use axum::{routing::get, Router};
use crate::AppState;

/// 健康检查路由
pub fn create_health_routes() -> Router<AppState> {
    Router::new()
    // .route("/health", get(handlers::health::health_check))
}
