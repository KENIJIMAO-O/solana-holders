use crate::baseline::getProgramAccounts::HttpClient;
use crate::database::postgresql::{DatabaseConfig, DatabaseConnection};
use crate::message_queue::message_queue::{Redis, RedisQueueConfig};
use crate::monitor::client::GrpcClient;
use crate::monitor::monitor::{Monitor, MonitorConfig, ReConnectConfig};
use crate::sync_controller::sync_controller::SyncController;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

pub mod baseline;
pub mod database;
pub mod message_queue;
pub mod monitor;
pub mod repositories;
pub mod sync_controller;
pub mod utils;

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
            message_queue.clone(),
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
        // 2.启动sync controller
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let database_config = DatabaseConfig::new_optimized(db_url);
        let database = Arc::new(DatabaseConnection::new(database_config).await.unwrap());

        let http_rpc = std::env::var("RPC_URL").unwrap();
        let http_client = Arc::new(HttpClient::new(http_rpc).unwrap());

        let sync_controller =
            SyncController::new(message_queue.clone(), database.clone(), http_client.clone());
        let sync_controller = {
            let mut sync_controller = sync_controller.clone();
            let token = cancellation_token.child_token();
            tokio::spawn(async move {
                debug!("[-] Starting sync controller...");
                if let Err(e) = sync_controller.consume_events_from_queue(token).await {
                    error!("Monitor error: {:?}", e);
                }

                debug!("Monitor completed");
            })
        };

        // 3.启动baseline
        // todo: 现在baseline成功获取了数据，但是还没有整合到数据库中

        let results = tokio::join!(monitor, sync_controller);
        // 检查任务结果
        if let Err(e) = results.0 {
            warn!("Monitor task panicked: {:?}", e);
        }
        if let Err(e) = results.1 {
            warn!("SyncController task panicked: {:?}", e);
        }
        Ok(())
    }
}
