use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use solana_holders::Server;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let console_subscriber = fmt::layer()
        // .with_target(false)
        // .with_level(false)
        .with_writer(std::io::stdout);
    tracing_subscriber::registry()
        // 添加控制台订阅者，并应用 EnvFilter
        .with(
            console_subscriber.with_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info,rustls=warn,sqlx=warn,hyper=warn,tokio=warn".into()),
            ),
        )
        .init();

    let server = Server {};
    match server.run().await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("server run error: {}", e);
        }
    }
}