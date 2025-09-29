use std::env;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use solana_holders::monitor::client::GrpcClient;
use solana_holders::monitor::monitor::{Monitor, MonitorConfig, ReConnectConfig, TokenEvent};

#[tokio::main]
async fn main() {
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

    dotenv::dotenv().ok();
    let monitor_config = MonitorConfig::new();
    let rpc_url = env::var("RPC_URL").unwrap();
    let client = GrpcClient::new(&rpc_url);
    let (event_sender, event_receiver) = mpsc::unbounded_channel::<TokenEvent>();
    let re_connect_config = ReConnectConfig::default();

    let mut onchain_monitor = Monitor::new(
        monitor_config,
        client,
        event_sender,
        re_connect_config,
    );

    let cancellation_token = CancellationToken::new();
    let token = cancellation_token.child_token();


    let result  = onchain_monitor.run_with_reconnect(token).await;
}