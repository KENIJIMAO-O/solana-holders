use std::env;

#[derive(Debug, Clone)]
pub struct KafkaQueueConfig {
    // Kafka broker 地址
    pub bootstrap_servers: String,

    // Topic 配置
    pub token_event_topic: String,
    pub baseline_topic: String,

    // Consumer Group 配置
    pub token_event_consumer_group: String,
    pub baseline_consumer_group: String,

    // Producer 配置
    pub producer_timeout_ms: String,

    // Consumer 配置
    pub consumer_session_timeout_ms: String,
    pub consumer_enable_auto_commit: String,
}

impl Default for KafkaQueueConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            token_event_topic: "token-events".to_string(),
            baseline_topic: "baseline-tasks".to_string(),
            token_event_consumer_group: "token-event-consumer".to_string(),
            baseline_consumer_group: "baseline-consumer".to_string(),
            producer_timeout_ms: "5000".to_string(),
            consumer_session_timeout_ms: "6000".to_string(),
            consumer_enable_auto_commit: "false".to_string(),
        }
    }
}

impl KafkaQueueConfig {
    /// 从环境变量创建配置，不存在则使用默认值
    pub fn from_env() -> Self {
        Self {
            bootstrap_servers: env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            token_event_topic: env::var("KAFKA_TOKEN_EVENT_TOPIC")
                .unwrap_or_else(|_| "token-events".to_string()),
            baseline_topic: env::var("KAFKA_BASELINE_TOPIC")
                .unwrap_or_else(|_| "baseline-tasks".to_string()),
            token_event_consumer_group: env::var("KAFKA_TOKEN_EVENT_CONSUMER_GROUP")
                .unwrap_or_else(|_| "token-event-consumer".to_string()),
            baseline_consumer_group: env::var("KAFKA_BASELINE_CONSUMER_GROUP")
                .unwrap_or_else(|_| "baseline-consumer".to_string()),
            producer_timeout_ms: env::var("KAFKA_PRODUCER_TIMEOUT_MS")
                .unwrap_or_else(|_| "5000".to_string()),
            consumer_session_timeout_ms: env::var("KAFKA_CONSUMER_SESSION_TIMEOUT_MS")
                .unwrap_or_else(|_| "6000".to_string()),
            consumer_enable_auto_commit: "false".to_string(),
        }
    }
}
