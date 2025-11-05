use crate::baseline::HttpClient;
use anyhow::anyhow;
use reqwest::Client;
use serde_json::Value;

impl HttpClient {
    pub async fn get_sol_scan_holder(&self, mint: &str) -> anyhow::Result<u64> {
        let base_url = std::env::var("BASE_URL").unwrap();
        let url = format!("{}{}", base_url, mint);

        let response_text = self.get_token_meta(&url).await?;

        match serde_json::from_str::<Value>(&response_text) {
            Ok(v) => {
                // 像字典一样通过 key 访问
                // v["data"]["holder"] 会返回一个 Value

                // .as_u64() 尝试将其转换为 u64，返回一个 Option<u64>
                if let Some(holder_count) = v["data"]["holder"].as_u64() {
                    println!("✅ 成功提取 -> Holder count: {}", holder_count);
                    Ok(holder_count)
                } else {
                    eprintln!("'holder' 字段不存在或不是一个 u64 类型");
                    Err(anyhow!("'holder' 字段不存在或不是一个 u64 类型"))
                }
            }
            Err(e) => {
                eprintln!("JSON 解析失败: {}", e);
                Err(anyhow!("JSON 解析失败: {}", e))
            }
        }
    }

    async fn get_token_meta(&self, url: &str) -> Result<String, reqwest::Error> {
        let response = self
            .http_client
            .get(url)
            .header("content-type", "application/json")
            .header("token", &self.sol_scan_token) // "token" 是自定义的 header 名称
            .send()
            .await?;

        if !response.status().is_success() {
            eprintln!("请求失败，状态码: {}", response.status());
        }

        let response_text = response.text().await?;
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

        let mint = "DuWbi8VHLJBbUQ7f6vJmkneFESD5tANDPkKXogDQpump";

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
