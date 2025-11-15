use serde::Deserialize;
use std::time::Duration;
use crate::error::{BaselineError, Result};

pub mod get_program_accounts;
pub mod token_meta;

#[derive(Clone, Debug)]
pub struct HttpClient {
    rpc_url: String,
    sol_scan_token: String,
    http_client: reqwest::Client,
}

impl HttpClient {
    pub fn new(rpc_url: String, sol_scan_token: String) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(1600))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .map_err(|e| BaselineError::ClientCreationFailed(
                format!("Failed to create HTTP client: {}", e)
            ))?;
        Ok(Self {
            rpc_url,
            sol_scan_token,
            http_client,
        })
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        let rpc_url = std::env::var("RPC_URL")
            .expect("RPC_URL environment variable must be set");
        let sol_scan_token = std::env::var("SOLSCAN_API_KEY")
            .expect("SOLSCAN_API_KEY environment variable must be set");
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(1600))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
        Self {
            rpc_url,
            sol_scan_token,
            http_client,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct GetAccountInfoData {
    pub jsonrpc: String,
    pub result: AccountInfoResult,
    pub id: u64,
}

#[derive(Deserialize, Debug)]
pub struct AccountInfoResult {
    pub context: AccountInfoContextInfo,
    pub value: AccountInfoValue,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfoContextInfo {
    pub api_version: String,
    pub slot: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfoValue {
    pub data: Vec<String>, // ["", "base58"] 格式
    pub executable: bool,
    pub lamports: u64,
    pub owner: String,
    pub rent_epoch: u64,
    pub space: u64,
}

#[derive(Deserialize, Debug)]
pub struct GetProgramAccountsData {
    pub context: ContextInfo,
    pub value: ValueInfo,
}

// 使用 untagged enum 来兼容两种不同的 RPC 响应格式
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ValueInfo {
    // Helius V2 格式：value 是一个对象，包含 accounts/pagination_key/count
    V2(V2Value),
    // 原生 getProgramAccounts 格式：value 直接就是 Account 数组
    Legacy(Vec<Account>),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct V2Value {
    pub accounts: Vec<Account>,
    pub pagination_key: Option<String>,
    pub count: Option<usize>,
}

impl GetProgramAccountsData {
    // 提供统一的访问方法，无论是哪种格式都返回 accounts
    pub fn accounts(&self) -> &[Account] {
        match &self.value {
            ValueInfo::V2(v2) => &v2.accounts,
            ValueInfo::Legacy(accounts) => accounts,
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ContextInfo {
    api_version: String,
    slot: i64,
}

#[derive(Deserialize, Debug)]
pub struct Account {
    account: AccountInfo,
    pubkey: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    data: AccountData,
    executable: bool,
    lamports: u64,
    owner: String,
    rent_epoch: u64,
    space: u64,
}

#[derive(Deserialize, Debug)]
pub struct AccountData {
    parsed: ParsedData,
    program: String,
    space: u16,
}

#[derive(Deserialize, Debug)]
pub struct ParsedData {
    info: ParsedInfo,
    #[serde(rename = "type")]
    r#type: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ParsedInfo {
    is_native: bool,
    mint: String,
    owner: String,
    state: String,
    token_amount: TokenAmount,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TokenAmount {
    amount: String,
    decimals: u16,
    ui_amount: f64, // 这里的 f64 可能不够
    ui_amount_string: String,
}
