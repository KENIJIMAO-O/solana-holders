use std::sync::Arc;
use axum::Router;
use axum::routing::get;
use tokio::sync::Mutex;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tokio_util::sync::CancellationToken;
use once_cell::sync::Lazy;
use crate::baseline::HttpClient;
use crate::database::postgresql::{DatabaseConfig, DatabaseConnection};
use crate::monitor::client::GrpcClient;
use crate::monitor::monitor::{Monitor, MonitorConfig, ReConnectConfig};
use crate::sync_controller::sync_controller::SyncController;
use crate::clickhouse::clickhouse::ClickHouse;
use crate::kafka::{KafkaMessageQueue, KafkaQueueConfig};
use crate::error::{KafkaError, Result};
use crate::reconciliation::model::ReconciliationServer;

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

pub const APP_LOG_TARGET: &str = "my_app::app_log";
pub const REQUEST_LOG_TARGET: &str = "my_app::request_log";


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
    pub onchain_monitor: Arc<Mutex<Monitor>>,
    pub postgres: Arc<DatabaseConnection>,
    pub clickhouse: Arc<ClickHouse>,
    pub sync_controller: Arc<SyncController>,
    pub http_client: Arc<HttpClient>,
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
    let onchain_monitor = Arc::new(Mutex::new(Monitor::new(
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
        http_client
    })
}

pub async fn start_background_services(
    onchain_monitor: Arc<Mutex<Monitor>>,
    _postgres: Arc<DatabaseConnection>,
    _clickhouse: Arc<ClickHouse>,
    sync_controller: Arc<SyncController>,
    _http_client: Arc<HttpClient>,
    _cancellation_token: CancellationToken
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    // 1.启动monitor
    let monitor = {
        let monitor_clone = onchain_monitor.clone();
        let token = _cancellation_token.child_token();
        tokio::spawn(async move {
            app_debug!("[-] Starting on-chain monitoring with auto-reconnect...");
            let mut monitor = monitor_clone.lock().await;
            if let Err(e) = monitor.run_with_reconnect(token).await {
                app_error!("Monitor error: {:?}", e);
            }
            app_debug!("Monitor completed");
        })
    };

    app_info!("BIG_TOKEN_HOLDER_COUNT:{}", *BIG_TOKEN_HOLDER_COUNT);
    // 2.启动sync controller
    let sync_controller_events = {
        let sync_controller = sync_controller.clone();
        let token = _cancellation_token.child_token();
        tokio::spawn(async move {
            app_debug!("[-] Starting sync controller...");
            if let Err(e) = sync_controller.consume_events_from_queue(token).await {
                app_error!("Monitor error: {:?}", e);
            }
            app_debug!("Monitor completed");
        })
    };
    let sync_controller_baseline = {
        let sync_controller = sync_controller.clone();
        let token = _cancellation_token.child_token();
        tokio::spawn(async move {
            app_debug!("[-] Starting sync controller...");
            if let Err(e) = sync_controller
                .consume_baseline_mints_for_queue(token)
                .await
            {
                app_error!("Monitor error: {:?}", e);
            }
            app_debug!("Monitor completed");
        })
    };

    // 3.启动对账
    // let reconciliation_server = ReconciliationServer::new(
    //     _postgres.clone(),
    //     _clickhouse.clone(),
    //     _http_client.clone()
    // )?;
    //
    // let reconciliation = {
    //     let token = _cancellation_token.child_token();
    //     tokio::spawn(async move {
    //         app_debug!("[-] Starting reconciliation server...");
    //         if let Err(e) = reconciliation_server.start_with_cancellation(token).await {
    //             app_error!("reconciliation error: {:?}", e);
    //         }
    //         app_debug!("reconciliation completed");
    //     })
    // };

    // 返回所有后台任务的 JoinHandle
    Ok(vec![monitor, sync_controller_events, sync_controller_baseline])
}

/// 等待所有后台任务完成
pub async fn wait_for_background_tasks(handles: Vec<tokio::task::JoinHandle<()>>) {
    for (idx, handle) in handles.into_iter().enumerate() {
        if let Err(e) = handle.await {
            app_warn!("Background task {} panicked: {:?}", idx, e);
        } else {
            app_info!("Background task {} completed", idx);
        }
    }
}

pub async fn create_app(app_state: AppState) -> Router {
    Router::new()
        // Holder 查询路由
        .merge(routes::jsonrpc_routes())
        .route("/health", get(routes::health::health_check))
        // 中间件
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        )
        .with_state(app_state)
}
