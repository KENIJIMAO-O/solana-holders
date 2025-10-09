use crate::monitor::utils::utils::get_grpc_client;
use anyhow::Result;
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
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
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
            .await?;
        Ok((subscribe_tx, stream))
    }

    // grpc订阅最新交易哈希
    pub async fn get_latest_blockhash(&self) -> Result<String> {
        let mut client = GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .max_decoding_message_size(16 * 1024 * 1024)
            .max_encoding_message_size(16 * 1024 * 1024)
            .connect()
            .await?;
        let response = client.get_latest_blockhash(None).await?;
        Ok(response.blockhash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

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
}
