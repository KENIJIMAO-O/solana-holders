use solana_holders::{EVENT_LOG_TARGET, Server};
use std::env;
use tracing::Level;
use tracing_appender::non_blocking::NonBlockingBuilder;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use solana_holders::error::SolanaHoldersError;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let console_subscriber = fmt::layer()
        // .with_target(false)
        // .with_level(false)
        .with_writer(std::io::stdout)
        .with_filter(filter_fn(|metadata| {
            // 排除 event 日志
            !metadata.target().starts_with(EVENT_LOG_TARGET)
        }));

    // 临时订阅器，用于判断events到底是没收到还是收到了，写入数据库失败了
    let log_dir = env::var("LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
    let event_log = RollingFileAppender::new(Rotation::DAILY, &log_dir, "event.log");
    let (event_log_non_blocking_appender, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(10_000)
        .finish(event_log);
    let events_subscriber = fmt::layer()
        .with_target(false)
        .with_level(false)
        .with_writer(event_log_non_blocking_appender)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target().starts_with(EVENT_LOG_TARGET) && metadata.level() <= &Level::INFO
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

    let server = Server {};
    if let Err(e) = server.run().await {
        print_error_chain(&e);
        let exit_code = match &e {
            SolanaHoldersError::Config(_) => {
                tracing::error!("配置错误，请检查环境变量");
                2
            }
            _ if e.is_connection_error() => {
                tracing::error!("连接错误，请检查网络和服务状态");
                3
            }
            _ => {
                tracing::error!("运行时错误");
                1
            }
        };

        std::process::exit(exit_code);
    }
}

/// 打印完整的错误链
fn print_error_chain(err: &SolanaHoldersError) {
    tracing::error!("错误: {}", err);

    // 打印错误源链
    let mut source = std::error::Error::source(err);
    let mut indent = 1;

    while let Some(err) = source {
        tracing::error!("{:indent$}└─ 原因: {}", "", err, indent = indent * 2);
        source = std::error::Error::source(err);
        indent += 1;
    }
}

