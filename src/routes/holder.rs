use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use tracing::{error, info};
use crate::{AppState, BIG_TOKEN_HOLDER_COUNT};
use crate::database::repositories::mint_stats::MintStatsRepository;
use super::api::{ApiResponse, BatchHolderRequest, HolderInfo};

// ========== Handlers ==========

/// 查询单个 mint 的 holder 数量
///
/// 路由: GET /api/v1/holder/:mint_address
pub(crate) async fn get_holder_count(
    State(state): State<AppState>,
    Path(mint_address): Path<String>,
) -> (StatusCode, Json<ApiResponse<HolderInfo>>) {
    info!("📊 查询 holder 数量: mint={}", mint_address);

    // 步骤 1: 尝试从数据库查询
    match state.postgres.get_holder_account(&mint_address).await {
        Ok(holder_count) => {
            // 数据库中有数据，直接返回
            info!("✅ 数据库查询成功: mint={}, holders={}", mint_address, holder_count);
            let holder_info = HolderInfo {
                mint_address: mint_address.clone(),
                holder_count,
            };
            (StatusCode::OK, Json(ApiResponse::success(holder_info)))
        }
        Err(e) => {
            let error_msg = e.to_string();

            // 步骤 2: 如果数据库中没有（no rows），调用 process_single_baseline 获取
            if error_msg.contains("no rows") || error_msg.contains("RowNotFound") {
                info!("⚠️ 数据库中未找到 mint: {}, 尝试构建 baseline", mint_address);

                // 调用 process_single_baseline(is_find=true)
                // 该函数会：
                // 1. 调用 SolScan API 获取 holder_count（只调用一次）
                // 2. 判断是否为大代币
                //    - 大代币：直接返回 SolScan 的值
                //    - 小代币：构建 baseline，然后从数据库查询并返回
                match state
                    .sync_controller
                    .process_single_baseline(&mint_address, true)
                    .await
                {
                    Ok(holder_count) => {
                        info!("✅ 成功获取 holder count: mint={}, holders={}", mint_address, holder_count);
                        let holder_info = HolderInfo {
                            mint_address: mint_address.clone(),
                            holder_count,
                        };
                        (StatusCode::OK, Json(ApiResponse::success(holder_info)))
                    }
                    Err(baseline_err) => {
                        error!("❌ 获取 holder count 失败: mint={}, error={:?}", mint_address, baseline_err);
                        (
                            StatusCode::NOT_FOUND,
                            Json(ApiResponse::error(
                                "MINT_NOT_FOUND",
                                "Failed to fetch holder count for this mint"
                            ))
                        )
                    }
                }
            } else {
                // 其他数据库错误（非 no rows）
                error!("❌ 数据库错误: mint={}, error={:?}", mint_address, e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::error(
                        "DATABASE_ERROR",
                        "Internal server error"
                    ))
                )
            }
        }
    }
}

/// 批量查询 holders 数量 todo!: 暂时只支持，数据库中已有的代币，没有的代币直接返回空
/// 路由: POST /api/v1/holders/batch
pub(crate) async fn get_holders_batch(
    State(state): State<AppState>,
    Json(req): Json<BatchHolderRequest>,
) -> (StatusCode, Json<ApiResponse<Vec<HolderInfo>>>) {
    // 验证请求
    if req.mint_addresses.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::error(
                "INVALID_REQUEST",
                "mint_addresses cannot be empty"
            ))
        );
    }

    info!("📊 批量查询 {} 个 mints", req.mint_addresses.len());

    match state.postgres.get_holder_counts_batch(&req.mint_addresses).await {
        Ok(results) => {
            info!("✅ 批量查询成功: 返回 {} 个结果", results.len());

            let holder_infos: Vec<HolderInfo> = results
                .into_iter()
                .map(|(mint_address, holder_count)| HolderInfo {
                    mint_address,
                    holder_count,
                })
                .collect();

            (StatusCode::OK, Json(ApiResponse::success(holder_infos)))
        }
        Err(e) => {
            error!("❌ 批量查询失败: error={:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("DATABASE_ERROR", "Batch query failed"))
            )
        }
    }
}



