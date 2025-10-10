use crate::baseline::GetProgramAccountsData;
use anyhow::{Error, Result};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug)] // 使用 Debug trait 方便打印调试
pub struct TokenHolder {
    pub slot: u64,
    pub mint: String,
    pub owner: String,
    pub pubkey: String,
    pub balance: String, // 使用 String 类型存储 balance (amount) 是最安全的
    pub decimals: u16,
}

pub struct HttpClient {
    rpc_url: String,
    http_client: reqwest::Client,
}

impl HttpClient {
    pub fn new(rpc_url: String) -> anyhow::Result<Self> {
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(600))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create batch HTTP client: {}", e))?;
        Ok(Self {
            rpc_url,
            http_client,
        })
    }

    pub async fn get_program_accounts(&self, mint: &str) -> Result<Value, Error> {
        let request_body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getProgramAccounts",
                "params": [
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                {
                    "encoding": "jsonParsed",
                    "withContext": true,
                    "filters": [
                    {
                        "dataSize": 165
                    },
                    {
                        "memcmp": {
                        "offset": 0,
                        "bytes": mint
                        }
                    }
                    ]
                }
            ]
        });

        let response = self
            .http_client
            .post(&self.rpc_url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        // 解析响应
        let json_response: serde_json::Value = response.json().await?;

        // 提取result字段
        let result = json_response
            .get("result")
            .ok_or_else(|| anyhow::anyhow!("响应中没有result字段"))?;

        let get_program_accounts_result: GetProgramAccountsData =
            serde_json::from_value(result.clone())?;
        let slot = get_program_accounts_result.context.slot;
        println!("slot: {}", slot);

        let token_holders: Vec<TokenHolder> = get_program_accounts_result
            .value
            .into_iter()
            .map(|value_info| TokenHolder {
                slot,
                mint: value_info.account.data.parsed.info.mint,
                owner: value_info.account.data.parsed.info.owner,
                pubkey: value_info.pubkey,
                balance: value_info
                    .account
                    .data
                    .parsed
                    .info
                    .token_amount
                    .ui_amount_string,
                decimals: value_info.account.data.parsed.info.token_amount.decimals,
            })
            .collect();
        if let Some(first_holder) = token_holders.first() {
            println!("转换后的第一条数据: {:?}", first_holder);
        }

        // Ok(result) 我不能直接这样返回引用，因为当前引用的值在当前函数结束的时候就已经被释放了，所以返回的时候引用指向空值
        // 有一种情况rust允许函数返回引用，那就是这个返回的值是从函数外部传进来的，同时还得声明其生命周期（第一次具象化感受到了生命周期的作用）
        Ok(result.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_write_file() {
        let path = "getProgramAccounts.json";
        let mut output = File::create(path).unwrap();

        write!(output, "wuxizhi").unwrap();
    }

    #[tokio::test]
    async fn test_get_program_accounts() {
        dotenv::dotenv().ok();
        let rpc_url = std::env::var("RPC_URL").unwrap();
        let http_client = HttpClient::new(rpc_url).unwrap();

        let mint = "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY";
        let res = http_client.get_program_accounts(mint).await;

        let path = "getProgramAccounts.json";
        let mut output = File::create(path).unwrap();

        if let Ok(json_value) = res {
            println!("start to write res into file");
            if let Err(e) = serde_json::to_writer_pretty(&mut output, &json_value) {
                println!("🔥 写入JSON文件失败: {}", e);
            } else {
                println!("👍 文件 '{}' 写入成功!", path);
            }
        } else {
            println!("get_program_accounts failed");
        }
    }
}
