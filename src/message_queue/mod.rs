use anyhow::{Error, anyhow};
use deadpool_redis::{Config, Connection as DeadpoolConnection, Pool, Runtime};
use tracing::info;

// 暂时使用redis作为消息队列来试试
pub mod baseline_message_queue;
pub mod token_event_message_queue;

#[derive(Debug, Clone)]
pub struct RedisQueueConfig {
    pub token_event_namespace: String,
    pub token_event_consumer_group: String,
    pub baseline_namespace: String,
    pub baseline_consumer_group: String,
    pub stream_ttl: Option<i64>,
    pub max_stream_length: i64,
    pub stats_ttl: Option<i64>,
}

impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            token_event_namespace: "scheduler".to_string(),
            token_event_consumer_group: "token_event_consumer".to_string(),
            baseline_namespace: "baseline".to_string(),
            baseline_consumer_group: "baseline_consumer".to_string(),
            stream_ttl: Some(3600),
            max_stream_length: 200000,
            stats_ttl: Some(30 * 24 * 3600), // 30天TTL
        }
    }
}
#[derive(Debug, Clone)]
pub struct Redis {
    pub queue_connection_pool: Pool, // 消息队列连接池
    pub redis_queue_config: RedisQueueConfig,
}

impl Redis {
    /// 创建一个新的 Redis 服务实例，并初始化连接池
    pub async fn new(redis_url: &str, config: RedisQueueConfig) -> anyhow::Result<Self, Error> {
        // 正确方式：直接将 URL 字符串传给 from_url
        let cfg = Config::from_url(redis_url);

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| anyhow!("Failed to create Deadpool Redis pool: {}", e))?;

        info!("✅ [REDIS] Deadpool connection pool created successfully.");

        Ok(Self {
            queue_connection_pool: pool,
            redis_queue_config: config,
        })
    }

    /// 异步从 deadpool 中获取一个连接
    pub async fn get_connection(&self) -> anyhow::Result<DeadpoolConnection, Error> {
        let conn = self.queue_connection_pool.get().await?;
        Ok(conn)
    }
}
