use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use crate::message_queue::message_queue::{Redis, RedisQueueConfig};
use crate::monitor::client::GrpcClient;
use crate::monitor::monitor::{Monitor, MonitorConfig, ReConnectConfig};

pub mod baseline;
pub mod monitor;
pub mod message_queue;
pub mod database;
pub mod repositories;
pub mod sync_controller;

pub struct Server;

impl Server {
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        // 1.启动监控
        let monitor_config = MonitorConfig::new();
        let reconnect_config = ReConnectConfig::default();
        let grpc_client = GrpcClient::new(std::env::var("GRPC_URL").unwrap().as_str());

        // 创建消息队列
        let redis_url = std::env::var("REDIS_URL");
        let config = RedisQueueConfig::default();
        let message_queue = Arc::new(Redis::new(&redis_url.unwrap(), config).await.unwrap());
        let _ = message_queue.initialize_message_queue().await.unwrap();

        let onchain_monitor = Monitor::new(
            monitor_config,
            grpc_client,
            message_queue,
            reconnect_config,
        );

        // 创建全局取消令牌
        let cancellation_token = CancellationToken::new();
        let monitor = {
            let mut monitor = onchain_monitor.clone();
            let token = cancellation_token.child_token();
            tokio::spawn(async move {
                debug!("[-] Starting on-chain monitoring with auto-reconnect...");
                if let Err(e) = monitor.run_with_reconnect(token).await {
                    error!("Monitor error: {:?}", e);
                }
                debug!("Monitor completed");
            })
        };
        // todo: 现在缺的是monitor和消息队列以及数据库连接起来的过程
        // 2.启动baseline
        // todo: 现在baseline成功获取了数据，但是还没有整合到数据库中



        Ok(())
    }
}