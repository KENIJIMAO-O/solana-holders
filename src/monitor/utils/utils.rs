use crate::error::{GrpcError, ParseError, Result};
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status_client_types::{
    EncodedTransactionWithStatusMeta, UiTransactionEncoding,
};
use std::env;
use std::time::Duration;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::{convert_from, geyser::SubscribeUpdateTransactionInfo};

/// GRPC连接配置常量
pub struct GrpcConfig;

impl GrpcConfig {
    pub const CONNECT_TIMEOUT_SECS: u64 = 10;
    pub const REQUEST_TIMEOUT_SECS: u64 = 60;
    pub const MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024; // 8MB
}

pub async fn get_grpc_client() -> Result<GeyserGrpcClient<impl Interceptor>> {
    let grpc: String = env::var("GRPC_URL").expect("GRPC_URL must be set");
    GeyserGrpcClient::build_from_shared(grpc.to_string())
        .map_err(|e| GrpcError::ConnectionFailed {
            url: grpc.clone(),
            source: Box::new(e),
        })?
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| GrpcError::ConnectionFailed {
            url: grpc.clone(),
            source: Box::new(e),
        })?
        .connect_timeout(Duration::from_secs(GrpcConfig::CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(GrpcConfig::REQUEST_TIMEOUT_SECS))
        .max_decoding_message_size(GrpcConfig::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(GrpcConfig::MAX_MESSAGE_SIZE)
        .connect()
        .await
        .map_err(|e| GrpcError::ConnectionFailed {
            url: grpc,
            source: Box::new(e),
        }.into())
}

// 转换交易信息类型，方便下一步提取交易信息
pub fn convert_to_encoded_tx(
    tx_info: SubscribeUpdateTransactionInfo,
) -> Result<EncodedTransactionWithStatusMeta> {
    let tx_with_meta = convert_from::create_tx_with_meta(tx_info)
        .map_err(|_| ParseError::InvalidFormat("Failed to create transaction with meta".to_string()))?;

    tx_with_meta
        .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
        .map_err(|_e| ParseError::TransactionDecodeFailed {
            slot: 0,  // slot 信息在这里不可用
            sig: "unknown".to_string(),
        }.into())
}

// 将Vec<u8>类型的函数转换为String类型
pub fn txn_signature_to_string(txn_signature: Vec<u8>) -> Option<String> {
    Option::from(bs58::encode(txn_signature).into_string())
}

#[derive(Debug)]
pub struct CreateAccountInfo {
    pub instruction_type: u32,
    pub lamports: u64,
    pub space: u64,
    pub owner: Pubkey,
}
/// CreateAccount指令的数据结构常量
pub struct CreateAccountConstants;

impl CreateAccountConstants {
    pub const EXPECTED_DATA_LENGTH: usize = 52;
    pub const EXPECTED_INSTRUCTION_TYPE: u32 = 0;
    pub const OWNER_PUBKEY_SIZE: usize = 32;
    pub const OWNER_OFFSET: usize = 20;
}

/// 解析系统程序的CreateAccount指令数据
pub fn parse_create_account_instruction(data: &[u8]) -> std::result::Result<CreateAccountInfo, &'static str> {
    // 验证数据长度
    if data.len() != CreateAccountConstants::EXPECTED_DATA_LENGTH {
        return Err("Invalid data length for CreateAccount instruction");
    }

    // 解析并验证指令类型
    let instruction_type = parse_u32_le(&data[0..4]);
    if instruction_type != CreateAccountConstants::EXPECTED_INSTRUCTION_TYPE {
        return Err("Not a CreateAccount instruction");
    }

    // 解析各字段
    let lamports = parse_u64_le(&data[4..12]);
    let space = parse_u64_le(&data[12..20]);
    let owner = parse_pubkey(&data[CreateAccountConstants::OWNER_OFFSET..])?;

    Ok(CreateAccountInfo {
        instruction_type,
        lamports,
        space,
        owner,
    })
}

/// 辅助函数：解析小端序u32
fn parse_u32_le(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

/// 辅助函数：解析小端序u64
fn parse_u64_le(bytes: &[u8]) -> u64 {
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// 辅助函数：解析公钥
fn parse_pubkey(bytes: &[u8]) -> std::result::Result<Pubkey, &'static str> {
    if bytes.len() < CreateAccountConstants::OWNER_PUBKEY_SIZE {
        return Err("Insufficient data for pubkey");
    }

    let owner_bytes: [u8; 32] = bytes[..CreateAccountConstants::OWNER_PUBKEY_SIZE]
        .try_into()
        .map_err(|_| "Failed to parse pubkey bytes")?;

    Ok(Pubkey::new_from_array(owner_bytes))
}

use rust_decimal::Decimal;
use std::str::FromStr;

pub fn subtract_as_decimal(a_str: &str, b_str: &str) -> std::result::Result<String, rust_decimal::Error> {
    // 1. 使用 Decimal::from_str 将字符串解析为 Decimal 类型
    //    这也返回一个 Result
    let a = Decimal::from_str(a_str)?;
    let b = Decimal::from_str(b_str)?;

    // 2. Decimal 类型重载了减法操作符，可以直接相减
    let result = a - b;

    Ok(result.to_string())
}
