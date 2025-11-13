use crate::monitor::utils::utils::get_grpc_client;
use crate::error::Result;
use futures::Sink;
use futures::channel::mpsc;
use std::collections::HashMap;
use std::time::Duration;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::tonic::codegen::tokio_stream::Stream;
use yellowstone_grpc_proto::{
    geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterTransactions, SubscribeUpdate,
    },
    tonic::Status,
};

type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
/// grpc需要的区块过滤map
type BlocksFilterMap = HashMap<String, SubscribeRequestFilterBlocks>;

/// notice: 我们不在GrpcClient保存一个client，而是对于每一个订阅创建一个新的客户端是因为可能在多进程内各订阅任务导致冲突
#[derive(Debug, Clone)]
pub struct GrpcClient {
    endpoint: String,
}

impl GrpcClient {
    pub fn new(endpoint: &str) -> GrpcClient {
        Self {
            endpoint: endpoint.to_string(),
        }
    }

    // gprc订阅区块
    pub async fn subscribe_block(
        &self,
        account_include: Vec<String>,       // 关注的地址
        include_transactions: Option<bool>, // 是否包含所有交易
        include_entries: Option<bool>,      // 默认false
        commitment: CommitmentLevel,        // commitment级别
    ) -> Result<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = std::result::Result<SubscribeUpdate, Status>>,
    )> {
        // 1.构建client
        let mut client = get_grpc_client().await?;

        // 2.构建需要过滤的block
        let mut blocks: BlocksFilterMap = HashMap::new();
        blocks.insert(
            "client".to_owned(),
            SubscribeRequestFilterBlocks {
                account_include,
                include_transactions,
                include_accounts: None, // 移除这个参数，设置为None
                include_entries,
            },
        );

        // 3.构建request
        let subscribe_request = SubscribeRequest {
            blocks,
            commitment: Some(commitment.into()),
            ..Default::default()
        };

        // 返回流
        let (subscribe_tx, stream) = client
            .subscribe_with_request(Some(subscribe_request))
            .await
            .map_err(|e| crate::error::GrpcError::SubscriptionFailed(format!("Failed to subscribe: {}", e)))?;
        Ok((subscribe_tx, stream))
    }

    pub async fn subscribe_transaction(
        &self,
        account_include: Vec<String>,  // 包含在内的地址相关交易都会收到
        account_exclude: Vec<String>,  // 不包含这些地址的相关交易都会收到
        account_required: Vec<String>, // 必须要包含的地址
        commitment: CommitmentLevel,
    ) -> Result<impl Stream<Item = std::result::Result<SubscribeUpdate, Status>>> {
        // client
        let mut client = GeyserGrpcClient::build_from_shared(self.endpoint.clone())
            .map_err(|e| crate::error::GrpcError::ConnectionFailed {
                url: self.endpoint.clone(),
                source: Box::new(e),
            })?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| crate::error::GrpcError::ConnectionFailed {
                url: self.endpoint.clone(),
                source: Box::new(e),
            })?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .connect()
            .await
            .map_err(|e| crate::error::GrpcError::ConnectionFailed {
                url: self.endpoint.clone(),
                source: Box::new(e),
            })?;

        // 过滤规则
        let mut transactions: TransactionsFilterMap = HashMap::new();
        transactions.insert(
            "client".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include,
                account_exclude,
                account_required,
            },
        );

        // request
        let subscribe_request = SubscribeRequest {
            transactions,
            commitment: Some(commitment.into()),
            ..Default::default()
        };

        // 返回流
        let (_, stream) = client
            .subscribe_with_request(Some(subscribe_request))
            .await
            .map_err(|e| crate::error::GrpcError::SubscriptionFailed(format!("Failed to subscribe: {}", e)))?;

        Ok(stream)
    }

    // grpc订阅最新交易哈希
    pub async fn get_latest_blockhash(&self) -> Result<String> {
        let mut client = GeyserGrpcClient::build_from_shared(self.endpoint.clone())
            .map_err(|e| crate::error::GrpcError::ConnectionFailed {
                url: self.endpoint.clone(),
                source: Box::new(e),
            })?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| crate::error::GrpcError::ConnectionFailed {
                url: self.endpoint.clone(),
                source: Box::new(e),
            })?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .max_decoding_message_size(16 * 1024 * 1024)
            .max_encoding_message_size(16 * 1024 * 1024)
            .connect()
            .await
            .map_err(|e| crate::error::GrpcError::ConnectionFailed {
                url: self.endpoint.clone(),
                source: Box::new(e),
            })?;
        let response = client.get_latest_blockhash(None).await
            .map_err(|e| crate::error::GrpcError::StreamError(format!("Failed to get latest blockhash: {}", e)))?;
        Ok(response.blockhash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::utils::constant::{TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID_2022};
    use crate::monitor::utils::utils::txn_signature_to_string;
    use chrono::Local;
    use std::env;
    use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
    use yellowstone_grpc_proto::tonic::codegen::tokio_stream::StreamExt;

    /// 创建测试用的GrpcClient
    fn create_test_grpc_client() -> GrpcClient {
        dotenv::dotenv().ok();
        GrpcClient::new(&*env::var("GRPC_URL").unwrap())
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_latest_blockhash() {
        dotenv::dotenv().ok();
        let client = create_test_grpc_client();

        let res = client.get_latest_blockhash().await.unwrap();
        println!("res {:?}", res);
    }

    /// 测试并发处理的数据结构
    #[test]
    fn test_concurrent_data_structures() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        // 测试原子计数器
        let counter = Arc::new(AtomicU64::new(0));
        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // 测试并发HashMap
        let mut concurrent_map: HashMap<String, u64> = HashMap::new();
        concurrent_map.insert("processed_blocks".to_string(), 100);
        concurrent_map.insert("processed_transactions".to_string(), 500);

        assert_eq!(concurrent_map.get("processed_blocks"), Some(&100));
        assert_eq!(concurrent_map.get("processed_transactions"), Some(&500));
        assert_eq!(concurrent_map.len(), 2);
    }

    /// 测试subscribe_transaction订阅TOKEN_PROGRAM相关交易
    /// 只打印基本信息，不做处理，用于测试gRPC交易订阅速度
    /// 统计每个区块中收到的交易数量
    #[tokio::test]
    async fn test_subscribe_token_transactions() {
        dotenv::dotenv().ok();
        let client = create_test_grpc_client();

        // 订阅TOKEN_PROGRAM_ID和TOKEN_PROGRAM_ID_2022
        let account_include = vec![
            TOKEN_PROGRAM_ID.to_string(),
            TOKEN_PROGRAM_ID_2022.to_string(),
        ];

        println!("🔌 正在连接 gRPC 订阅交易...");
        println!("📋 订阅账户: {:?}", account_include);

        let mut stream = client
            .subscribe_transaction(
                account_include,
                vec![], // account_exclude
                vec![], // account_required
                CommitmentLevel::Confirmed,
            )
            .await
            .unwrap();

        println!("✅ gRPC 订阅成功，开始接收交易（只打印，不做任何处理）");

        let mut tx_count = 0u64;
        let start_time = std::time::Instant::now();

        // 统计每个slot的交易数量
        let mut slot_tx_count: HashMap<u64, u64> = HashMap::new();
        let mut last_slot = 0u64;

        loop {
            if let Some(Ok(data)) = stream.next().await {
                if let Some(update) = data.update_oneof {
                    match update {
                        UpdateOneof::Transaction(tx) => {
                            tx_count += 1;
                            let now = Local::now().format("%H:%M:%S%.3f");

                            let slot = tx.slot;
                            let signature = txn_signature_to_string(
                                tx.transaction.as_ref().unwrap().signature.clone(),
                            )
                            .unwrap_or_else(|| "unknown".to_string());

                            // 更新当前slot的交易计数
                            *slot_tx_count.entry(slot).or_insert(0) += 1;

                            // 如果切换到新的slot，打印上一个slot的统计
                            if last_slot > 0 && slot != last_slot {
                                let last_slot_tx_count =
                                    slot_tx_count.get(&last_slot).copied().unwrap_or(0);
                                println!(
                                    "📦 Slot {} 完成: 共收到 {} 笔交易",
                                    last_slot, last_slot_tx_count
                                );
                            }

                            // println!(
                            //     "[{}] 📨 交易 #{} | Slot: {} | Sig: {}...{}",
                            //     now,
                            //     tx_count,
                            //     slot,
                            //     &signature[..8],
                            //     &signature[signature.len()-8..]
                            // );

                            last_slot = slot;

                            // 每100笔统计一次速度
                            if tx_count % 100 == 0 {
                                let elapsed = start_time.elapsed().as_secs_f64();
                                let tps = tx_count as f64 / elapsed;
                                println!(
                                    "📊 统计: 总交易={}, 耗时={:.2}s, 平均速度={:.2}tx/s",
                                    tx_count, elapsed, tps
                                );
                            }
                        }
                        UpdateOneof::Ping(_) => {
                            println!("🏓 收到 Ping");
                        }
                        UpdateOneof::Pong(_) => {
                            println!("🏓 收到 Pong");
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
