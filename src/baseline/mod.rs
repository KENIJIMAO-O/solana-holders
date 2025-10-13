use serde::Deserialize;

pub mod getProgramAccounts;

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
    context: ContextInfo,
    value: Vec<ValueInfo>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ContextInfo {
    api_version: String,
    slot: i64,
}

#[derive(Deserialize, Debug)]
pub struct ValueInfo {
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
