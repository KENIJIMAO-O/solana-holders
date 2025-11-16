use crate::baseline::{GetAccountInfoData, GetProgramAccountsData, HttpClient};
use crate::monitor::utils::constant::{TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID_2022};
use crate::error::{BaselineError, Result};
use futures::stream::Stream;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::pin::Pin;
use std::str::FromStr;
use crate::app_info;

#[derive(Debug, Deserialize, Serialize)] // 使用 Debug trait 方便打印调试
pub struct TokenHolder {
    pub slot: i64,
    pub mint: String,    // token address
    pub owner: String,   // token holder
    pub pubkey: String,  // token account
    pub balance: String, // 使用 String 类型存储 balance (amount) 是最安全的
    pub decimals: u16,
}

impl HttpClient {
    // baseline 入口
    pub async fn get_token_holders(&self, mint: &str) -> Result<Vec<TokenHolder>> {
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
            .await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getAccountInfo".to_string(),
                source: Box::new(e),
            })?;

        // 解析响应
        let json_response: serde_json::Value = response.json().await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getAccountInfo response parsing".to_string(),
                source: Box::new(e),
            })?;

        // 检查是否有error字段
        if let Some(error) = json_response.get("error") {
            return Err(BaselineError::RpcCallFailed {
                method: "getAccountInfo".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("RPC returned error: {}", error)
                )),
            }.into());
        }

        // 检查result字段是否存在
        if json_response.get("result").is_none() {
            return Err(BaselineError::RpcCallFailed {
                method: "getAccountInfo".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("RPC response missing 'result' field. Response: {}", json_response)
                )),
            }.into());
        }

        let get_program_accounts_result: GetAccountInfoData = 
            serde_json::from_value(json_response.clone())
                .map_err(|e|{
                    BaselineError::ParseFailed {
                    operation: "parse getAccountInfo response".to_string(),
                    source: e,
                }})?;
        let owner = get_program_accounts_result.result.value.owner.clone();

        let token_holders = if owner == TOKEN_PROGRAM_ID.to_string() {
            self.get_program_accounts(mint).await
        } else if owner == TOKEN_PROGRAM_ID_2022.to_string() {
            self.get_program_accounts_2022(mint).await
        } else {
            Err(BaselineError::InvalidTokenProgram {
                program_id: owner,
            }.into())
        };

        token_holders
    }

    // 目前这个函数只能针对owner为TokenProgram的spl token，但对于owner为TokenProgram2022的spl token还没法获取
    pub async fn get_program_accounts(&self, mint: &str) -> Result<Vec<TokenHolder>> {
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
            .await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getProgramAccounts".to_string(),
                source: Box::new(e),
            })?;

        // 解析响应
        let json_response: serde_json::Value = response.json().await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getProgramAccounts response parsing".to_string(),
                source: Box::new(e),
            })?;

        // 提取result字段
        let result = json_response
            .get("result")
            .ok_or_else(|| BaselineError::RpcCallFailed {
                method: "getProgramAccounts".to_string(),
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "响应中没有result字段")),
            })?;

        let get_program_accounts_result: GetProgramAccountsData = 
            serde_json::from_value(result.clone())
                .map_err(|e| BaselineError::ParseFailed {
                    operation: "parse getProgramAccounts result".to_string(),
                    source: e,
                })?;
        let slot = get_program_accounts_result.context.slot;

        let token_holders: Vec<TokenHolder> = get_program_accounts_result
            .accounts()
            .iter()
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
                        app_info!("parse balance error: {}", e);
                        false
                    }
                }
            })
            .map(|value_info| TokenHolder {
                slot,
                mint: value_info.account.data.parsed.info.mint.clone(),
                owner: value_info.account.data.parsed.info.owner.clone(),
                pubkey: value_info.pubkey.clone(),
                balance: value_info
                    .account
                    .data
                    .parsed
                    .info
                    .token_amount
                    .ui_amount_string
                    .clone(),
                decimals: value_info.account.data.parsed.info.token_amount.decimals,
            })
            .collect();

        // Ok(result) 我不能直接这样返回引用，因为当前引用的值在当前函数结束的时候就已经被释放了，所以返回的时候引用指向空值
        // 有一种情况rust允许函数返回引用，那就是这个返回的值是从函数外部传进来的，同时还得声明其生命周期（第一次具象化感受到了生命周期的作用）
        Ok(token_holders)
    }

    pub async fn get_program_accounts_2022(&self, mint: &str) -> Result<Vec<TokenHolder>> {
        let request_body = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getProgramAccounts",
                "params": [
                    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
                {
                    "encoding": "jsonParsed",
                    "withContext": true,
                    "filters": [
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
            .await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getProgramAccounts (Token-2022)".to_string(),
                source: Box::new(e),
            })?;

        // 解析响应
        let json_response: serde_json::Value = response.json().await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getProgramAccounts (Token-2022) response parsing".to_string(),
                source: Box::new(e),
            })?;

        // 提取result字段
        let result = json_response
            .get("result")
            .ok_or_else(|| BaselineError::RpcCallFailed {
                method: "getProgramAccounts (Token-2022)".to_string(),
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "响应中没有result字段")),
            })?;

        let get_program_accounts_result: GetProgramAccountsData = 
            serde_json::from_value(result.clone())
                .map_err(|e| BaselineError::ParseFailed {
                    operation: "parse getProgramAccounts (Token-2022) result".to_string(),
                    source: e,
                })?;
        let slot = get_program_accounts_result.context.slot;

        let token_holders: Vec<TokenHolder> = get_program_accounts_result
            .accounts()
            .iter()
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
                        app_info!("parse balance error: {}", e);
                        false
                    }
                }
            })
            .map(|value_info| TokenHolder {
                slot,
                mint: value_info.account.data.parsed.info.mint.clone(),
                owner: value_info.account.data.parsed.info.owner.clone(),
                pubkey: value_info.pubkey.clone(),
                balance: value_info
                    .account
                    .data
                    .parsed
                    .info
                    .token_amount
                    .ui_amount_string
                    .clone(),
                decimals: value_info.account.data.parsed.info.token_amount.decimals,
            })
            .collect();

        Ok(token_holders)
    }

    /// 流式获取token holders，不会一次性将所有数据加载到内存
    /// 返回一个Stream，调用者可以逐个处理TokenHolder
    pub fn get_program_accounts_v2_stream(
        &self,
        mint: &str,
    ) -> Pin<Box<dyn Stream<Item = Result<TokenHolder>> + Send + '_>> {
        let mint = mint.to_string();
        let client = self.clone();

        Box::pin(async_stream::stream! {
            let mut pagination_key: Option<String> = None;
            let program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string();
            let mut total_count = 0usize;

            loop {
                let mut params_obj = serde_json::Map::new();
                params_obj.insert("encoding".to_string(), json!("jsonParsed"));
                params_obj.insert("withContext".to_string(), json!(true));
                params_obj.insert("limit".to_string(), json!(5000));
                params_obj.insert(
                    "filters".to_string(),
                    json!([
                        { "dataSize": 165 },
                        {
                            "memcmp": {
                                "offset": 0,
                                "bytes": mint
                            }
                        }
                    ]),
                );

                if let Some(key) = &pagination_key {
                    params_obj.insert("paginationKey".to_string(), json!(key));
                }

                let request_body = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getProgramAccountsV2",
                    "params": [program_id, params_obj]
                });

                let response = match client.http_client
                    .post(&client.rpc_url)
                    .header("Content-Type", "application/json")
                    .json(&request_body)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        yield Err(BaselineError::RpcCallFailed {
                            method: "getProgramAccountsV2".to_string(),
                            source: Box::new(e),
                        }.into());
                        return;
                    }
                };

                // 先获取响应状态码
                let status = response.status();

                // 获取原始文本，以便在解析失败时打印
                let response_text = match response.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        yield Err(BaselineError::RpcCallFailed {
                            method: "getProgramAccountsV2 read response text".to_string(),
                            source: Box::new(e),
                        }.into());
                        return;
                    }
                };

                let json_response: serde_json::Value = match serde_json::from_str(&response_text) {
                    Ok(j) => j,
                    Err(e) => {
                        // 打印详细错误信息
                        println!("\n❌❌❌ JSON解析失败! ❌❌❌");
                        println!("HTTP状态码: {}", status);
                        println!("解析错误: {}", e);
                        println!("响应长度: {} bytes", response_text.len());

                        // 打印前500字符和后500字符
                        if response_text.len() > 1000 {
                            println!("响应内容前500字符:\n{}", &response_text[..500]);
                            println!("\n响应内容后500字符:\n{}", &response_text[response_text.len()-500..]);
                        } else {
                            println!("完整响应内容:\n{}", response_text);
                        }
                        println!("❌❌❌❌❌❌❌❌❌❌❌❌\n");

                        yield Err(BaselineError::ParseFailed {
                            operation: "parse getProgramAccountsV2 JSON response".to_string(),
                            source: e,
                        }.into());
                        return;
                    }
                };

                let result = match json_response.get("result") {
                    Some(r) => r.clone(),
                    None => {
                        println!("result:{:?}", json_response);
                        yield Err(BaselineError::RpcCallFailed {
                            method: "getProgramAccountsV2".to_string(),
                            source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "响应中没有result字段")),
                        }.into());
                        return;
                    }
                };

                let get_program_accounts_result: GetProgramAccountsData = 
                    match serde_json::from_value(result) {
                        Ok(r) => r,
                        Err(e) => {
                            yield Err(BaselineError::ParseFailed {
                                operation: "deserialize getProgramAccountsV2 result".to_string(),
                                source: e,
                            }.into());
                            return;
                        }
                    };

                let slot = get_program_accounts_result.context.slot;

                // 提取 pagination_key（只有 V2 格式才有）
                pagination_key = match &get_program_accounts_result.value {
                    crate::baseline::ValueInfo::V2(v2) => v2.pagination_key.clone(),
                    crate::baseline::ValueInfo::Legacy(_) => None,
                };

                // 逐个yield TokenHolder，不累积在内存中
                for value_info in get_program_accounts_result.accounts() {
                    let balance_str = &value_info.account.data.parsed.info.token_amount.ui_amount_string;

                    let should_include = match Decimal::from_str(balance_str) {
                        Ok(dec) => dec > Decimal::ZERO,
                        Err(e) => {
                            app_info!("parse balance error: {}", e);
                            false
                        }
                    };

                    if should_include {
                        let holder = TokenHolder {
                            slot,
                            mint: value_info.account.data.parsed.info.mint.clone(),
                            owner: value_info.account.data.parsed.info.owner.clone(),
                            pubkey: value_info.pubkey.clone(),
                            balance: value_info.account.data.parsed.info.token_amount.ui_amount_string.clone(),
                            decimals: value_info.account.data.parsed.info.token_amount.decimals,
                        };

                        total_count += 1;
                        yield Ok(holder);
                    }
                }

                if pagination_key.is_none() {
                    app_info!("获取 'getProgramAccountsV2' 完成，总计 {} holders", total_count);
                    break;
                } else {
                    app_info!("获取到下一页的 'paginationKey'，已获取 {} holders，继续...", total_count);
                }
            }
        })
    }

    /// todo!: 现在的情况是这样，get_program_accounts_v2虽然可以分也来做，但是随之也带来了新的问题，不同页数之间的slot不一致的问题，
    /// 其实也引入了新的不稳定性，毕竟数据是基于helius来获取的
    pub async fn get_program_accounts_v2(&self, mint: &str) -> Result<Vec<TokenHolder>> {
        use futures::stream::StreamExt;

        let mut all_token_holders: Vec<TokenHolder> = Vec::new();
        let mut stream = self.get_program_accounts_v2_stream(mint);

        while let Some(result) = stream.next().await {
            all_token_holders.push(result?);
        }

        println!("holder count: {}", all_token_holders.len());
        Ok(all_token_holders)
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
        let http_client = HttpClient::default();

        let mint = "D3thCZnicLHLnqRM2Ycom11hS648TUgWa9WX8ihCdm39";
        let res = http_client.get_token_holders(mint).await;

        let path = "getProgramAccounts.json";
        let mut output = File::create(path).unwrap();

        if let Ok(json_value) = res {
            println!("token:{:?} has holder:{:?}", mint, json_value.len());
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
    async fn test_get_program_accounts_v1() {
        use std::time::Instant;

        let start_time = Instant::now();
        println!("Start time: {:?}", start_time);

        dotenv::dotenv().ok();
        let rpc_url = std::env::var("SOLANA_NODE_RPC_URL").unwrap();
        let http_client = HttpClient::default();

        let mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

        let res = http_client.get_program_accounts(mint).await;

        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);

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
            // 打印错误信息
            if let Err(e) = res.as_ref() {
                println!("🚨 失败原因: {:?}", e);
            }
        }

        println!("\n=======================================================");
        println!("⏰ 任务总耗时: {} seconds", duration.as_secs_f64());
        println!("=======================================================\n");
    }

    #[tokio::test]
    async fn test_get_program_accounts_2022() {
        dotenv::dotenv().ok();
        let rpc_url = std::env::var("RPC_URL").unwrap();
        let http_client = HttpClient::default();

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

    #[tokio::test]
    async fn test_get_program_accounts_v2() {
        use futures::stream::StreamExt;
        use std::io::BufWriter;
        use std::time::Instant;

        let start_time = Instant::now();
        println!("Start time: {:?}", start_time);

        dotenv::dotenv().ok();
        let rpc_url = std::env::var("SOLANA_NODE_RPC_URL").unwrap();
        let base_url = std::env::var("BASE_URL").unwrap();
        println!("HTTP URL: {}", rpc_url);
        let sol_scan_token = std::env::var("SOLSCAN_API_KEY").unwrap();
        let http_client = HttpClient::new(rpc_url, base_url, sol_scan_token).unwrap();

        // DFL1zNkaGPWm1BqAVqRjCZvHmwTFrEaJtbzJWgseoNJh EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
        let mint = "4Cnk9EPnW5ixfLZatCPJjDB1PUtcRpVVgTQukm9epump"; // usdc

        let path = "getProgramAccountsV2.json";
        let file = match File::create(path) {
            Ok(f) => f,
            Err(e) => {
                println!("🔥 创建文件失败: {}", e);
                return;
            }
        };
        let mut writer = BufWriter::new(file);

        println!("开始流式写入文件...");
        let mut stream = http_client.get_program_accounts_v2_stream(mint);
        let mut count = 0usize;
        let mut is_first = true;

        if let Err(e) = writer.write_all(b"[\n") {
            println!("🔥 写入失败: {}", e);
            return;
        }

        while let Some(result) = stream.next().await {
            match result {
                Ok(holder) => {
                    if !is_first {
                        if let Err(e) = writer.write_all(b",\n") {
                            println!("🔥 写入失败: {}", e);
                            return;
                        }
                    }
                    is_first = false;

                    if let Err(e) = serde_json::to_writer(&mut writer, &holder) {
                        println!("🔥 序列化失败: {}", e);
                        return;
                    }

                    count += 1;
                    if count % 1000 == 0 {
                        println!("已写入 {} holders...", count);
                    }
                }
                Err(e) => {
                    println!("🔥 获取数据失败: {}", e);
                    return;
                }
            }
        }

        if let Err(e) = writer.write_all(b"\n]") {
            println!("🔥 写入失败: {}", e);
            return;
        }

        if let Err(e) = writer.flush() {
            println!("🔥 刷新缓冲失败: {}", e);
            return;
        }

        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);

        println!("👍 文件 '{}' 写入成功!", path);
        println!("总计 {} holders", count);
        println!("duration: {:?}", duration);
    }

    #[tokio::test]
    async fn test_get_account_info() -> Result<()>{ 
        dotenv::dotenv().ok();
        let mint = "SnJNWtX6yHaEmxdR3nbyXrB3nyqXYmhsm18orScs1vu";

        let rpc_url = std::env::var("RPC_URL").unwrap();
        let http_client = HttpClient::default();

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
        let response = http_client
            .http_client
            .post(rpc_url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getAccountInfo".to_string(),
                source: Box::new(e),
            })?;

        // 解析响应
        let json_response: serde_json::Value = response.json().await
            .map_err(|e| BaselineError::RpcCallFailed {
                method: "getAccountInfo response parsing".to_string(),
                source: Box::new(e),
            })?;

        let get_program_accounts_result: GetAccountInfoData = 
            serde_json::from_value(json_response.clone())
                .map_err(|e|{
                    BaselineError::ParseFailed {
                        operation: "parse getAccountInfo response".to_string(),
                        source: e,
                    }})?;
        let owner = get_program_accounts_result.result.value.owner.clone();

        println!("owner :{:#?}", owner);
        Ok(())
    }
}