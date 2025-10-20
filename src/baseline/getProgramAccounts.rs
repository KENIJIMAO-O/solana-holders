use crate::baseline::{GetAccountInfoData, GetProgramAccountsData};
use crate::monitor::utils::constant::{TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID_2022};
use anyhow::{Error, Result, anyhow};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::str::FromStr;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Deserialize, Serialize)] // 使用 Debug trait 方便打印调试
pub struct TokenHolder {
    pub slot: i64,
    pub mint: String,    // token address
    pub owner: String,   // token holder
    pub pubkey: String,  // token account
    pub balance: String, // 使用 String 类型存储 balance (amount) 是最安全的
    pub decimals: u16,
}

#[derive(Clone, Debug)]
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
            .timeout(Duration::from_secs(1600))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create batch HTTP client: {}", e))?;
        Ok(Self {
            rpc_url,
            http_client,
        })
    }

    // baseline 入口
    pub async fn get_token_holders(&self, mint: &str) -> Result<Vec<TokenHolder>, Error> {
        // 1.判断当前代币类型
        let request_body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                mint,
                {"encoding": "base64"}
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

        let get_program_accounts_result: GetAccountInfoData =
            serde_json::from_value(json_response.clone())?;
        let owner = get_program_accounts_result.result.value.owner.clone();

        let token_holders = if owner == TOKEN_PROGRAM_ID.to_string() {
            self.get_program_accounts(mint).await
        } else if owner == TOKEN_PROGRAM_ID_2022.to_string() {
            self.get_program_accounts_2022(mint).await
        } else {
            Err(anyhow!("unexpected token program id: {}", owner))
        };

        token_holders
    }

    // 目前这个函数只能针对owner为TokenProgram的spl token，但对于owner为TokenProgram2022的spl token还没法获取
    pub async fn get_program_accounts(&self, mint: &str) -> Result<Vec<TokenHolder>, Error> {
        let request_body = json!({
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

        let token_holders: Vec<TokenHolder> = get_program_accounts_result
            .value
            .into_iter()
            .filter(|value_info| {
                // 过滤balance，仅保留大于0的token_account
                let balance_str = &value_info
                    .account
                    .data
                    .parsed
                    .info
                    .token_amount
                    .ui_amount_string;

                // 尝试将字符串解析为 Decimal，然后判断是否大于 0
                match Decimal::from_str(balance_str).map(|dec| dec > Decimal::ZERO) {
                    Ok(true) => true,
                    Ok(false) => false,
                    Err(e) => {
                        info!("parse balance error: {}", e);
                        false
                    }
                }
            })
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

        // Ok(result) 我不能直接这样返回引用，因为当前引用的值在当前函数结束的时候就已经被释放了，所以返回的时候引用指向空值
        // 有一种情况rust允许函数返回引用，那就是这个返回的值是从函数外部传进来的，同时还得声明其生命周期（第一次具象化感受到了生命周期的作用）
        Ok(token_holders)
    }

    pub async fn get_program_accounts_2022(&self, mint: &str) -> Result<Vec<TokenHolder>, Error> {
        let request_body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getProgramAccounts",
                "params": [
                    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
                {
                    "encoding": "jsonParsed",
                    "withContext": true,
                    "filters": [
                    // {
                    //     "dataSize": 182
                    // },
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

        let token_holders: Vec<TokenHolder> = get_program_accounts_result
            .value
            .into_iter()
            .filter(|value_info| {
                // 过滤balance，仅保留大于0的token_account
                let balance_str = &value_info
                    .account
                    .data
                    .parsed
                    .info
                    .token_amount
                    .ui_amount_string;

                // 尝试将字符串解析为 Decimal，然后判断是否大于 0
                match Decimal::from_str(balance_str).map(|dec| dec > Decimal::ZERO) {
                    Ok(true) => true,
                    Ok(false) => false,
                    Err(e) => {
                        info!("parse balance error: {}", e);
                        false
                    }
                }
            })
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

        Ok(token_holders)
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
    async fn test_get_token_holders() {
        dotenv::dotenv().ok();
        let rpc_url = std::env::var("RPC_URL").unwrap();
        let http_client = HttpClient::new(rpc_url).unwrap();

        let mint = "2oQNkePakuPbHzrVVkQ875WHeewLHCd2cAwfwiLQbonk";
        let res = http_client.get_token_holders(mint).await;

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

    #[tokio::test]
    async fn test_get_program_accounts_2022() {
        dotenv::dotenv().ok();
        let rpc_url = std::env::var("RPC_URL").unwrap();
        let http_client = HttpClient::new(rpc_url).unwrap();

        let mint = "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn";
        let res = http_client.get_program_accounts_2022(mint).await;

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
