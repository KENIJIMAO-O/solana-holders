use crate::baseline::HttpClient;
use crate::error::{BaselineError, ConfigError, Result};
use serde_json::Value;

impl HttpClient {
    pub async fn get_sol_scan_holder(&self, mint: &str) -> Result<u64> {
        let base_url = std::env::var("BASE_URL")
            .expect("BASE_URL environment variable must be set");
        let url = format!("{}{}", base_url, mint);

        let response_text = self.get_token_meta(&url).await?;

        let v: Value = serde_json::from_str(&response_text)
            .map_err(|e| BaselineError::ParseFailed {
                operation: "parse solscan token meta response".to_string(),
                source: e,
            })?;

        // 像字典一样通过 key 访问
        // v["data"]["holder"] 会返回一个 Value
        // .as_u64() 尝试将其转换为 u64，返回一个 Option<u64>
        let holder_count = v["data"]["holder"].as_u64()
            .ok_or_else(|| BaselineError::RpcCallFailed {
                method: "get_sol_scan_holder".to_string(),
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "'holder' 字段不存在或不是一个 u64 类型")),
            })?;

        println!("✅ 成功提取 -> Holder count: {}", holder_count);
        Ok(holder_count)
    }

    async fn get_token_meta(&self, url: &str) -> Result<String> {
        let response = self
            .http_client
            .get(url)
            .header("content-type", "application/json")
            .header("token", &self.sol_scan_token) // "token" 是自定义的 header 名称
            .send()
            .await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "get_token_meta (solscan)".to_string(),
                source: Box::new(e),
            })?;

        if !response.status().is_success() {
            eprintln!("请求失败，状态码: {}", response.status());
        }

        let response_text = response.text().await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "get_token_meta (solscan) read response".to_string(),
                source: Box::new(e),
            })?;
        Ok(response_text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_sol_scan_holder() {
        dotenv::dotenv().ok();
        let http_client = HttpClient::default();

        let mint = "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY";

        let res = http_client.get_sol_scan_holder(mint).await;
        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_get_token_meta() {
        dotenv::dotenv().ok();
        let http_client = HttpClient::default();

        let base_url = "https://pro-api.solscan.io/v2.0/token/meta?address=";
        let mint = "DuWbi8VHLJBbUQ7f6vJmkneFESD5tANDPkKXogDQpump";

        let url = format!("{}{}", base_url, mint);

        let res = http_client.get_token_meta(&url).await;
    }
}
