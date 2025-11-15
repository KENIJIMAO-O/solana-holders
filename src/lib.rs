use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use crate::baseline::HttpClient;
use crate::database::postgresql::{DatabaseConfig, DatabaseConnection};
use crate::monitor::client::GrpcClient;
use crate::monitor::new_monitor::{MonitorConfig, ReConnectConfig};
use crate::monitor::new_monitor::NewMonitor;
use crate::sync_controller::sync_controller::SyncController;
use crate::clickhouse::clickhouse::ClickHouse;
use crate::kafka::{KafkaMessageQueue, KafkaQueueConfig};
use crate::error::{KafkaError, Result};

pub mod baseline;
pub mod database;
pub mod kafka;
pub mod monitor;
pub mod reconciliation;
pub mod sync_controller;
pub mod utils;
pub mod clickhouse;
pub mod error;
pub struct Server;

pub const EVENT_LOG_TARGET: &str = "my_app::event_log";

impl Server {
    pub async fn run(&self) -> Result<()> {
        // 1.创建消息队列
        let kafka_queue_config = KafkaQueueConfig::default();
        let message_queue = Arc::new(
            KafkaMessageQueue::new(kafka_queue_config)
                .map_err(|_| KafkaError::ProducerCreationFailed("producer create failed when create queue".to_string()))?
        );

        // 2.启动监控
        let monitor_config = MonitorConfig::new();
        let reconnect_config = ReConnectConfig::default();
        let grpc_url = std::env::var("GRPC_URL")
            .expect("GRPC_URL environment variable must be set");
        let grpc_client = GrpcClient::new(&grpc_url);

        let mut onchain_monitor = NewMonitor::new(
            monitor_config,
            grpc_client,
            message_queue.clone(),
            reconnect_config,
        );

        // 3.全局配置
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

        // 4.启动monitor
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

        // 5.创建数据库 postgres, clickhouse
        let db_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL environment variable must be set");
        let database_config = DatabaseConfig::new_optimized(db_url);
        let database = Arc::new(DatabaseConnection::new(database_config).await?);

        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .expect("CLICKHOUSE_URL environment variable must be set");
        let database_name = std::env::var("CLICKHOUSE_DATABASE_NAME")
            .expect("CLICKHOUSE_DATABASE_NAME environment variable must be set");
        let password = std::env::var("CLICKHOUSE_PASSWORD")
            .expect("CLICKHOUSE_PASSWORD environment variable must be set");
        
        let clickhouse_client = Arc::new(ClickHouse::new(&clickhouse_url, &database_name, &password));

        // 6.启动 sync controller
        let http_client = Arc::new(HttpClient::default());
        let sync_controller =
            SyncController::new(message_queue.clone(), database.clone(), clickhouse_client.clone(), http_client.clone());
        let sync_controller_events = {
            let sync_controller = sync_controller.clone();
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
            let sync_controller = sync_controller.clone();
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
        // 7.启动对账服务
        // let reconciliation_server = ReconciliationServer::new(
        //     database.clone(),
        //     clickhouse_client.clone(),
        //     http_client.clone()
        // )?;
        //
        // let reconciliation = {
        //     let token = cancellation_token.child_token();
        //     tokio::spawn(async move {
        //         debug!("[-] Starting reconciliation server...");
        //         if let Err(e) = reconciliation_server.start_with_cancellation(token).await {
        //             error!("reconciliation error: {:?}", e);
        //         }
        //         debug!("reconciliation completed");
        //     })
        // };


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
        // if let Err(e) = results.3 {
        //     warn!("Reconciliation server task panicked: {:?}", e);
        // }

        // 优雅关闭数据库连接池
        warn!("Closing database connection pool...");
        database.pool.close().await;
        warn!("Database connections closed");

        Ok(())
    }
}
