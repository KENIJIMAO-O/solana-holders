use crate::baseline::HttpClient;
use crate::database::postgresql::{DatabaseConfig, DatabaseConnection};
use crate::monitor::client::GrpcClient;
use crate::monitor::monitor::{MonitorConfig, ReConnectConfig};
use crate::monitor::new_monitor::NewMonitor;
use crate::sync_controller::sync_controller::SyncController;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use crate::clickhouse::clickhouse::ClickHouse;
use crate::kafka::{KafkaMessageQueue, KafkaQueueConfig};

pub mod baseline;
pub mod database;
pub mod kafka;
pub mod message_queue;
pub mod monitor;
pub mod reconciliation;
pub mod repositories;
pub mod sync_controller;
pub mod utils;
pub mod clickhouse;
pub struct Server;

pub const EVENT_LOG_TARGET: &str = "my_app::event_log";

impl Server {
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        // 1.启动监控
        let monitor_config = MonitorConfig::new();
        let reconnect_config = ReConnectConfig::default();
        let grpc_client = GrpcClient::new(std::env::var("GRPC_URL").unwrap().as_str());

        // 创建消息队列
        // let redis_url = std::env::var("REDIS_URL");
        // let config = RedisQueueConfig::default();
        // let message_queue = Arc::new(Redis::new(&redis_url.unwrap(), config).await.unwrap());
        // let _ = message_queue.initialize_message_queue().await.unwrap();
        // let _ = message_queue.init_baseline_queue().await.unwrap();
        //
        // let mut onchain_monitor = Monitor::new(
        //     monitor_config,
        //     grpc_client,
        //     message_queue.clone(),
        //     reconnect_config,
        // );

        let kafka_queue_config = KafkaQueueConfig::default();
        let message_queue = Arc::new(KafkaMessageQueue::new(kafka_queue_config).unwrap());
        println!("monitor_config:{:?}", monitor_config);
        println!("grpc_client:{:?}", grpc_client);
        // println!("message_queue:{:?}", message_queue);
        println!("reconnect_config:{:?}", reconnect_config);
        let mut onchain_monitor = NewMonitor::new(
            monitor_config,
            grpc_client,
            message_queue.clone(),
            reconnect_config,
        );

        // 创建全局取消令牌
        let cancellation_token = CancellationToken::new();

        // 设置信号处理（Ctrl+C）
        let signal_token = cancellation_token.clone();
        tokio::spawn(async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    warn!("Received Ctrl+C, initiating graceful shutdown...");
                    signal_token.cancel();
                }
                Err(err) => {
                    error!("Failed to listen for Ctrl+C: {}", err);
                }
            }
        });
        let monitor = {
            let token = cancellation_token.child_token();
            tokio::spawn(async move {
                debug!("[-] Starting on-chain monitoring with auto-reconnect...");
                if let Err(e) = onchain_monitor.run_with_reconnect(token).await {
                    error!("Monitor error: {:?}", e);
                }
                debug!("Monitor completed");
            })
        };

        // 2.postgres, clickhouse
        let db_url = std::env::var("DATABASE_URL").unwrap();
        let database_config = DatabaseConfig::new_optimized(db_url);
        let database = Arc::new(DatabaseConnection::new(database_config).await.unwrap());

        let clickhouse_url = std::env::var("CLICKHOUSE_URL").unwrap();
        let database_name = std::env::var("CLICKHOUSE_DATABASE_NAME").unwrap();
        let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap();
        let clickhouse_client = Arc::new(ClickHouse::new(&clickhouse_url, &database_name, &password));

        let http_client = Arc::new(HttpClient::default());

        // 3.启动 sync controller
        let sync_controller =
            SyncController::new(message_queue.clone(), database.clone(), clickhouse_client.clone(), http_client.clone());
        let sync_controller_events = {
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
        let sync_controller_baseline = {
            let mut sync_controller = sync_controller.clone();
            let token = cancellation_token.child_token();
            tokio::spawn(async move {
                debug!("[-] Starting sync controller...");
                if let Err(e) = sync_controller
                    .consume_baseline_mints_for_queue(token)
                    .await
                {
                    error!("Monitor error: {:?}", e);
                }
                debug!("Monitor completed");
            })
        };

        let results = tokio::join!(monitor, sync_controller_events, sync_controller_baseline);
        // let results = tokio::join!(monitor);

        // 检查任务结果
        if let Err(e) = results.0 {
            warn!("Monitor task panicked: {:?}", e);
        }
        if let Err(e) = results.1 {
            warn!("SyncController events task panicked: {:?}", e);
        }
        if let Err(e) = results.2 {
            warn!("SyncController baseline task panicked: {:?}", e);
        }

        // 优雅关闭数据库连接池
        warn!("Closing database connection pool...");
        database.pool.close().await;
        warn!("Database connections closed");

        Ok(())
    }
}
