use std::sync::Arc;
use axum::Router;
use tokio::sync::Mutex;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use once_cell::sync::Lazy;
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
pub mod routes;

pub struct Server;

pub const EVENT_LOG_TARGET: &str = "my_app::event_log";

pub static BIG_TOKEN_HOLDER_COUNT: Lazy<u64> = Lazy::new(|| {
    // 这部分代码只会在第一次被访问时运行
    std::env::var("BIG_TOKEN_HOLDER_COUNT")
        .unwrap()
        .parse::<u64>()
        .unwrap()
});

/// 应用状态结构体，包含所有共享的服务
#[derive(Clone)]
pub struct AppState {
    pub message_queue: Arc<KafkaMessageQueue>,
    pub onchain_monitor: Arc<Mutex<NewMonitor>>,
    pub postgres: Arc<DatabaseConnection>,
    pub clickhouse: Arc<ClickHouse>,
    pub sync_controller: Arc<SyncController>,
}

pub async fn initialize_system(
    grpc_url: String,
    db_url: String,
    clickhouse_url: String,
    clickhouse_name: String,
    clickhouse_password: String,
) -> Result<AppState> {
    // 1.创建消息队列
    let kafka_queue_config = KafkaQueueConfig::default();
    let message_queue = Arc::new(
        KafkaMessageQueue::new(kafka_queue_config)
            .map_err(|_| KafkaError::ProducerCreationFailed("producer create failed when create queue".to_string()))?
    );

    let monitor_config = MonitorConfig::new();
    let reconnect_config = ReConnectConfig::default();
    let grpc_client = GrpcClient::new(&grpc_url);

    // 2.创建监控
    let onchain_monitor = Arc::new(Mutex::new(NewMonitor::new(
        monitor_config,
        grpc_client,
        message_queue.clone(),
        reconnect_config,
    )));

    // 3.创建数据库 postgres, clickhouse
    let database_config = DatabaseConfig::new_optimized(db_url);
    let database = Arc::new(DatabaseConnection::new(database_config).await?);
    let clickhouse_client = Arc::new(ClickHouse::new(&clickhouse_url, &clickhouse_name, &clickhouse_password));

    // 4.创建sync_controller
    let http_client = Arc::new(HttpClient::default());
    let sync_controller =
        Arc::new(SyncController::new(message_queue.clone(), database.clone(), clickhouse_client.clone(), http_client.clone()));

    Ok(AppState {
        message_queue,
        onchain_monitor,
        postgres: database,
        clickhouse: clickhouse_client,
        sync_controller,
    })
}

pub async fn start_background_services(
    onchain_monitor: Arc<Mutex<NewMonitor>>,
    _postgres: Arc<DatabaseConnection>,
    _clickhouse: Arc<ClickHouse>,
    sync_controller: Arc<SyncController>,
    _cancellation_token: CancellationToken,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    // 1.启动monitor
    let monitor = {
        let monitor_clone = onchain_monitor.clone();
        let token = _cancellation_token.child_token();
        tokio::spawn(async move {
            debug!("[-] Starting on-chain monitoring with auto-reconnect...");
            let mut monitor = monitor_clone.lock().await;
            if let Err(e) = monitor.run_with_reconnect(token).await {
                error!("Monitor error: {:?}", e);
            }
            debug!("Monitor completed");
        })
    };

    info!("BIG_TOKEN_HOLDER_COUNT:{}", *BIG_TOKEN_HOLDER_COUNT);
    // 2.启动sync controller
    let sync_controller_events = {
        let sync_controller = sync_controller.clone();
        let token = _cancellation_token.child_token();
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
        let token = _cancellation_token.child_token();
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

    // 3.启动对账
    // let reconciliation_server = ReconciliationServer::new(
    //     postgres.clone(),
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

    // 返回所有后台任务的 JoinHandle
    Ok(vec![monitor, sync_controller_events, sync_controller_baseline])
}

/// 等待所有后台任务完成
pub async fn wait_for_background_tasks(handles: Vec<tokio::task::JoinHandle<()>>) {
    for (idx, handle) in handles.into_iter().enumerate() {
        if let Err(e) = handle.await {
            warn!("Background task {} panicked: {:?}", idx, e);
        } else {
            info!("Background task {} completed", idx);
        }
    }
}

// impl Server {
//     pub async fn run(&self) -> Result<()> {
//         // 1.创建消息队列
//         let kafka_queue_config = KafkaQueueConfig::default();
//         let message_queue = Arc::new(
//             KafkaMessageQueue::new(kafka_queue_config)
//                 .map_err(|_| KafkaError::ProducerCreationFailed("producer create failed when create queue".to_string()))?
//         );
//
//         // 2.启动监控
//         let monitor_config = MonitorConfig::new();
//         let reconnect_config = ReConnectConfig::default();
//         let grpc_url = std::env::var("GRPC_URL")
//             .expect("GRPC_URL environment variable must be set");
//         let grpc_client = GrpcClient::new(&grpc_url);
//
//         let mut onchain_monitor = NewMonitor::new(
//             monitor_config,
//             grpc_client,
//             message_queue.clone(),
//             reconnect_config,
//         );
//
//         // 3.全局配置
//         // 创建全局取消令牌
//         let cancellation_token = CancellationToken::new();
//
//         // 设置信号处理（Ctrl+C）
//         let signal_token = cancellation_token.clone();
//         tokio::spawn(async move {
//             match tokio::signal::ctrl_c().await {
//                 Ok(()) => {
//                     warn!("Received Ctrl+C, initiating graceful shutdown...");
//                     signal_token.cancel();
//                 }
//                 Err(err) => {
//                     error!("Failed to listen for Ctrl+C: {}", err);
//                 }
//             }
//         });
//
//         // 4.启动monitor
//         let monitor = {
//             let token = cancellation_token.child_token();
//             tokio::spawn(async move {
//                 debug!("[-] Starting on-chain monitoring with auto-reconnect...");
//                 if let Err(e) = onchain_monitor.run_with_reconnect(token).await {
//                     error!("Monitor error: {:?}", e);
//                 }
//                 debug!("Monitor completed");
//             })
//         };
//
//         // 5.创建数据库 postgres, clickhouse
//         let db_url = std::env::var("DATABASE_URL")
//             .expect("DATABASE_URL environment variable must be set");
//         let database_config = DatabaseConfig::new_optimized(db_url);
//         let database = Arc::new(DatabaseConnection::new(database_config).await?);
//
//         let clickhouse_url = std::env::var("CLICKHOUSE_URL")
//             .expect("CLICKHOUSE_URL environment variable must be set");
//         let database_name = std::env::var("CLICKHOUSE_DATABASE_NAME")
//             .expect("CLICKHOUSE_DATABASE_NAME environment variable must be set");
//         let password = std::env::var("CLICKHOUSE_PASSWORD")
//             .expect("CLICKHOUSE_PASSWORD environment variable must be set");
//
//         let clickhouse_client = Arc::new(ClickHouse::new(&clickhouse_url, &database_name, &password));
//
//         // 6.启动 sync controller
//         let http_client = Arc::new(HttpClient::default());
//         let sync_controller =
//             SyncController::new(message_queue.clone(), database.clone(), clickhouse_client.clone(), http_client.clone());
//         let sync_controller_events = {
//             let sync_controller = sync_controller.clone();
//             let token = cancellation_token.child_token();
//             tokio::spawn(async move {
//                 debug!("[-] Starting sync controller...");
//                 if let Err(e) = sync_controller.consume_events_from_queue(token).await {
//                     error!("Monitor error: {:?}", e);
//                 }
//                 debug!("Monitor completed");
//             })
//         };
//         let sync_controller_baseline = {
//             let sync_controller = sync_controller.clone();
//             let token = cancellation_token.child_token();
//             tokio::spawn(async move {
//                 debug!("[-] Starting sync controller...");
//                 if let Err(e) = sync_controller
//                     .consume_baseline_mints_for_queue(token)
//                     .await
//                 {
//                     error!("Monitor error: {:?}", e);
//                 }
//                 debug!("Monitor completed");
//             })
//         };
//         // 7.启动对账服务
//         // let reconciliation_server = ReconciliationServer::new(
//         //     database.clone(),
//         //     clickhouse_client.clone(),
//         //     http_client.clone()
//         // )?;
//         //
//         // let reconciliation = {
//         //     let token = cancellation_token.child_token();
//         //     tokio::spawn(async move {
//         //         debug!("[-] Starting reconciliation server...");
//         //         if let Err(e) = reconciliation_server.start_with_cancellation(token).await {
//         //             error!("reconciliation error: {:?}", e);
//         //         }
//         //         debug!("reconciliation completed");
//         //     })
//         // };
//
//
//         let results = tokio::join!(monitor, sync_controller_events, sync_controller_baseline);
//         // let results = tokio::join!(monitor);
//
//         // 检查任务结果
//         if let Err(e) = results.0 {
//             warn!("Monitor task panicked: {:?}", e);
//         }
//         if let Err(e) = results.1 {
//             warn!("SyncController events task panicked: {:?}", e);
//         }
//         if let Err(e) = results.2 {
//             warn!("SyncController baseline task panicked: {:?}", e);
//         }
//         // if let Err(e) = results.3 {
//         //     warn!("Reconciliation server task panicked: {:?}", e);
//         // }
//
//         // 优雅关闭数据库连接池
//         warn!("Closing database connection pool...");
//         database.pool.close().await;
//         warn!("Database connections closed");
//
//         Ok(())
//     }
// }

pub async fn create_app(app_state: AppState) -> Router {
    Router::new()
        // Holder 查询路由
        .merge(routes::jsonrpc_routes())
        // 中间件
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        )
        .with_state(app_state)
}
