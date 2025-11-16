pub mod health;
pub mod api;        // API 数据结构
pub mod holder;     // Holder 路由

use axum::{routing::post, Router};
use axum::routing::get;
use crate::AppState;

pub fn jsonrpc_routes() -> Router<AppState> {
    Router::new()
        // 单个查询
        .route("/api/v1/holder/:mint_address", get(holder::get_holder_count))
        // 批量查询
        .route("/api/v1/holders/batch", post(holder::get_holders_batch))
}