// 配置 jemalloc 作为全局内存分配器（解决内存碎片化问题）
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use solana_holders::{initialize_system, start_background_services, create_app, wait_for_background_tasks, APP_LOG_TARGET, REQUEST_LOG_TARGET, app_info, app_warn, app_error};
use std::env;
use std::time::Duration;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{Level};
use tracing_appender::non_blocking::NonBlockingBuilder;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use solana_holders::error::SolanaHoldersError;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    // 请求日志
    let console_subscriber = fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_writer(std::io::stdout)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target().starts_with(APP_LOG_TARGET) && metadata.level() <= &Level::INFO
        }));

    // 系统日志
    let log_dir = env::var("LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
    let event_log = RollingFileAppender::new(Rotation::DAILY, &log_dir, "event.log");
    let (event_log_non_blocking_appender, _guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(10_000)
        .finish(event_log);
    let events_subscriber = fmt::layer()
        .with_target(false)
        .with_level(false)
        .with_writer(event_log_non_blocking_appender)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target().starts_with(REQUEST_LOG_TARGET) && metadata.level() <= &Level::INFO
        }));

    tracing_subscriber::registry()
        // 添加控制台订阅者，并应用 EnvFilter
        .with(
            console_subscriber.with_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info,rustls=warn,sqlx=warn,hyper=warn,tokio=warn".into()),
            ),
        )
        .with(events_subscriber)
        .init();

    std::env::var("HEALTH_CHECK_MINT").expect(
        "HEALTH_CHECK_MINT should be set",
    );

    let grpc_url = std::env::var("GRPC_URL")
        .expect("GRPC_URL environment variable must be set");
    let db_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL environment variable must be set");
    let clickhouse_url = std::env::var("CLICKHOUSE_URL")
        .expect("CLICKHOUSE_URL environment variable must be set");
    let database_name = std::env::var("CLICKHOUSE_DATABASE_NAME")
        .expect("CLICKHOUSE_DATABASE_NAME environment variable must be set");
    let password = std::env::var("CLICKHOUSE_PASSWORD")
        .expect("CLICKHOUSE_PASSWORD environment variable must be set");

    let app_state = initialize_system(
        grpc_url,
        db_url,
        clickhouse_url,
        database_name,
        password
    ).await.unwrap();
    // 创建全局取消令牌
    let cancellation_token = CancellationToken::new();

    // 启动后台服务并收集JoinHandle
    let background_handles = start_background_services(
        app_state.onchain_monitor.clone(),
        app_state.postgres.clone(),
        app_state.clickhouse.clone(),
        app_state.sync_controller.clone(),
        cancellation_token.clone(),
    ).await.unwrap();

    // 构建应用（克隆 app_state 因为 create_app 会消耗它）
    let app = create_app(app_state.clone()).await;

    let server_url = env::var("SERVER_URL").unwrap();
    let listener = tokio::net::TcpListener::bind(&server_url).await.unwrap();
    app_info!("Server starting on {}", server_url);

    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                app_warn!("Server error: {:?}", e);
            }
        }
        _ = signal::ctrl_c() => {
            app_info!("Received Ctrl+C, initiating graceful shutdown...");
        }
    }

    // 发送关闭信号
    cancellation_token.cancel();
    app_info!("Shutdown signal sent, waiting for background tasks...");

    // 等待所有后台任务完成，最多等15秒
    let shutdown_timeout = Duration::from_secs(15);
    let shutdown_result = tokio::time::timeout(
        shutdown_timeout,
        wait_for_background_tasks(background_handles),
    )
        .await;

    match shutdown_result {
        Ok(_) => {
            app_info!("All background tasks completed successfully");
        }
        Err(_) => {
            app_warn!("Shutdown timeout reached, some tasks may still be running");
        }
    }

    // 优雅关闭数据库连接池
    app_info!("Closing database connection pool...");
    app_state.postgres.pool.close().await;
    app_info!("Database connections closed. Shutdown complete.");
}


/// 打印完整的错误链
fn print_error_chain(err: &SolanaHoldersError) {
    app_error!("错误: {}", err);

    // 打印错误源链
    let mut source = std::error::Error::source(err);
    let mut indent = 1;

    while let Some(err) = source {
        app_error!("{:indent$}└─ 原因: {}", "", err, indent = indent * 2);
        source = std::error::Error::source(err);
        indent += 1;
    }
}