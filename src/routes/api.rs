use serde::{Deserialize, Serialize};

// ========== 请求模型 ==========

/// 批量查询请求
#[derive(Debug, Deserialize)]
pub struct BatchHolderRequest {
    pub mint_addresses: Vec<String>,
}

// ========== 响应模型 ==========

/// 统一 API 响应结构
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

/// API 错误结构
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

/// Holder 信息
#[derive(Debug, Serialize)]
pub struct HolderInfo {
    pub mint_address: String,
    pub holder_count: i64,
}

// ========== 辅助实现 ==========

impl<T> ApiResponse<T> {
    /// 创建成功响应
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    /// 创建错误响应
    pub fn error(code: &str, message: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(ApiError {
                code: code.to_string(),
                message: message.to_string(),
            }),
        }
    }
}
