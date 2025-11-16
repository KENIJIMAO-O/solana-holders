use crate::{app_error, app_info, AppState};
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthCheckResponse {
    pub status: String,
    pub message: String,
    pub details: HealthCheckDetails,
}

#[derive(Serialize)]
pub struct HealthCheckDetails {
    pub mint_address: String,
    pub baseline_slot: Option<i64>,
    pub check_timestamp: i64,
}

/// 健康检查路由
///
/// 通过对健康检查 mint 执行完整的 baseline 构建流程来验证系统各个组件是否正常：
/// 1. RPC 连接是否正常（get_token_holders）
/// 2. 数据库连接和写入是否正常（establish_baseline_atomic）
///
/// 注意：此检查会实际调用 RPC 并写入数据库，建议使用 holder 数量较少的 mint
pub async fn health_check(State(app_state): State<AppState>) -> impl IntoResponse {
    let health_mint = std::env::var("HEALTH_CHECK_MINT").expect(
        "HEALTH_CHECK_MINT should be set",
    );

    app_info!("🏥 开始健康检查: mint={}", health_mint);

    // 调用 build_baseline 来测试完整的系统功能
    match app_state
        .sync_controller
        .build_baseline(&health_mint)
        .await
    {
        Ok(baseline_slot) => {
            app_info!(
                "✅ 健康检查通过: mint={}, baseline_slot={}",
                health_mint,
                baseline_slot
            );

            let response = HealthCheckResponse {
                status: "healthy".to_string(),
                message: "All systems operational".to_string(),
                details: HealthCheckDetails {
                    mint_address: health_mint,
                    baseline_slot: Some(baseline_slot),
                    check_timestamp: chrono::Utc::now().timestamp(),
                },
            };

            (StatusCode::OK, Json(response))
        }
        Err(err) => {
            app_error!("❌ 健康检查失败: mint={}, error={:?}", health_mint, err);

            let response = HealthCheckResponse {
                status: "unhealthy".to_string(),
                message: format!("Health check failed: {}", err),
                details: HealthCheckDetails {
                    mint_address: health_mint,
                    baseline_slot: None,
                    check_timestamp: chrono::Utc::now().timestamp(),
                },
            };

            (StatusCode::SERVICE_UNAVAILABLE, Json(response))
        }
    }
}

