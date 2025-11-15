use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        let settings = config::Config::builder()
            .add_source(config::File::with_name("config/default"))
            // 也可以从环境变量覆盖
            .add_source(config::Environment::with_prefix("APP"))
            .build()
            .unwrap();

        let res = settings.get::<KafkaQueueConfig>("kafka").unwrap();

        Self {
            bootstrap_servers: res.bootstrap_servers,
            token_event_topic: res.token_event_topic,
            baseline_topic: res.baseline_topic,
            token_event_consumer_group: res.token_event_consumer_group,
            baseline_consumer_group: res.baseline_consumer_group,
            producer_timeout_ms: res.producer_timeout_ms,
            consumer_session_timeout_ms: res.consumer_session_timeout_ms,
            consumer_enable_auto_commit: res.consumer_enable_auto_commit,
        }
    }
}


#[test]
fn test_kafka_load_config() {
    let settings = config::Config::builder()
        .add_source(config::File::with_name("config/default"))
        // 也可以从环境变量覆盖
        .add_source(config::Environment::with_prefix("APP"))
        .build()
        .unwrap();

    let res = settings.get::<KafkaQueueConfig>("kafka").unwrap();
    println!("res: {:#?}", res);
}