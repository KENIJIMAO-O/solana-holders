use crate::kafka::config::KafkaQueueConfig;
use crate::monitor::new_monitor::TokenEvent;
use crate::utils::timer::TaskLogger;
use crate::error::{KafkaError, Result, ValidationError};
use futures::future;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Kafka 消息队列核心结构
#[derive(Clone)]
pub struct KafkaMessageQueue {
    producer: Arc<FutureProducer>,
    // 缓存不同 consumer_name 对应的 StreamConsumer
    // Key: consumer_name, Value: StreamConsumer
    consumers: Arc<Mutex<HashMap<String, Arc<StreamConsumer>>>>,
    config: KafkaQueueConfig,
}

impl KafkaMessageQueue {
    /// 创建新的 Kafka 消息队列实例
    pub fn new(config: KafkaQueueConfig) -> Result<Self> {
        // 创建 Producer
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", &config.producer_timeout_ms)
            .create()
            .map_err(|e| KafkaError::ProducerCreationFailed(format!("Failed to create Kafka producer: {}", e)))?;

        info!(
            "✅ [KAFKA] Producer created successfully, bootstrap servers: {}",
            config.bootstrap_servers
        );

        Ok(Self {
            producer: Arc::new(producer),
            consumers: Arc::new(Mutex::new(HashMap::new())),
            config,
        })
    }

    /// 获取或创建指定名称的 Consumer
    /// 如果 consumer 已存在则复用，否则创建新的
    async fn get_or_create_consumer(
        &self,
        consumer_name: &str,
        topic: &str,
        consumer_group: &str,
    ) -> Result<Arc<StreamConsumer>> {
        let mut consumers = self.consumers.lock().await;

        // 如果已经存在，直接返回
        if let Some(consumer) = consumers.get(consumer_name) {
            return Ok(Arc::clone(consumer));
        }

        // 不存在则创建新的 Consumer（优化版：批量拉取）
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("group.id", consumer_group)
            .set(
                "enable.auto.commit",
                &self.config.consumer_enable_auto_commit,
            )
            .set(
                "session.timeout.ms",
                &self.config.consumer_session_timeout_ms,
            )
            .set("auto.offset.reset", "earliest")
            // --- 核心性能优化：批量拉取 ---
            // 1. 每次 fetch 至少拉取 1MB 数据（而不是拉取单条消息）
            .set("fetch.min.bytes", "1048576") // 1 MB
            // 2. Broker 等待数据累积的最长时间（平衡延迟和吞吐）
            .set("fetch.wait.max.ms", "100") // 100ms
            // 3. 本地缓冲区：至少保持 10000 条消息在内存中
            .set("queued.min.messages", "10000")
            // 4. 增加每次 poll 返回的最大字节数（默认 1MB，提升到 10MB）
            .set("max.partition.fetch.bytes", "10485760") // 10 MB
            .create()
            .map_err(|e| KafkaError::ConsumerCreationFailed(format!("Failed to create Kafka consumer: {}", e)))?;

        // 订阅 topic
        consumer
            .subscribe(&[topic])
            .map_err(|e| KafkaError::ReceiveFailed(format!("Failed to subscribe to topic {}: {}", topic, e)))?;

        info!(
            "✅ [KAFKA] Consumer '{}' created and subscribed to topic '{}'",
            consumer_name, topic
        );

        let consumer_arc = Arc::new(consumer);
        consumers.insert(consumer_name.to_string(), Arc::clone(&consumer_arc));

        Ok(consumer_arc)
    }

    // ==================== TokenEvent Queue Methods ====================
    /// 批量发送 TokenEvent 到 Kafka（优化版：并发发送）
    /// 对应 Redis 的 batch_enqueue_holder_event
    pub async fn batch_enqueue_holder_event(
        &self,
        token_events: Vec<TokenEvent>,
        logger: &mut TaskLogger,
    ) -> Result<Vec<String>> {
        if token_events.is_empty() {
            return Err(ValidationError::EmptyData("token events".to_string()).into());
        }

        logger.log("start to batch sending messages to kafka");

        let mut payloads = Vec::with_capacity(token_events.len());
        let mut keys = Vec::with_capacity(token_events.len());

        for event in token_events {
            let payload = serde_json::to_string(&event)
                .map_err(|e| KafkaError::SerializationFailed(format!("Serialization error: {}", e)))?;

            // 使用 mint_address 作为 key，确保同一代币的消息进入同一分区（保证顺序）
            let key = event.mint_address.to_string();

            payloads.push(payload);
            keys.push(key);
        }

        // 并发发送所有消息（不等待）
        let mut send_futures = Vec::with_capacity(payloads.len());

        for (key, payload) in keys.iter().zip(payloads.iter()) {
            let record = FutureRecord::to(&self.config.token_event_topic)
                .key(key)
                .payload(payload);

            // 发送消息但不等待（返回 Future）
            let send_future = self.producer.send(record, Duration::from_secs(5));
            send_futures.push(send_future);
        }

        // 统一等待所有消息的发送结果
        let results = future::join_all(send_futures).await;

        let mut msg_ids = Vec::with_capacity(results.len());
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(delivery) => {
                    let msg_id = format!("{}:{}", delivery.partition, delivery.offset);
                    msg_ids.push(msg_id);
                }
                Err((e, _)) => {
                    warn!("Failed to send message #{} to Kafka: {}", idx, e);
                    return Err(KafkaError::SendFailed {
                        topic: self.config.token_event_topic.clone(),
                        reason: format!("Failed to send message #{}: {}", idx, e),
                    }.into());
                }
            }
        }

        logger.log(&format!(
            "successfully sent {} messages to kafka",
            msg_ids.len()
        ));
        Ok(msg_ids)
    }

    /// 批量消费 TokenEvent 从 Kafka
    /// 对应 Redis 的 batch_dequeue_holder_event
    pub async fn batch_dequeue_holder_event(
        &self,
        consumer_name: &str,
        max_count: usize,
        block_ms: usize,
        logger: &mut TaskLogger,
    ) -> Result<Vec<(String, TokenEvent)>> {
        logger.log("start to getting or creating consumer");

        // 获取或创建 consumer
        let consumer = self
            .get_or_create_consumer(
                consumer_name,
                &self.config.token_event_topic,
                &self.config.token_event_consumer_group,
            )
            .await?;

        logger.log("start to batch reading messages");

        let mut events = Vec::new();
        let timeout = Duration::from_millis(block_ms as u64);
        let deadline = Instant::now() + timeout;

        // 循环接收消息，直到达到 max_count 或超时
        while events.len() < max_count && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());

            if remaining.is_zero() {
                break;
            }

            // 使用 tokio timeout 包裹接收操作
            match tokio::time::timeout(remaining, consumer.recv()).await {
                Ok(Ok(msg)) => {
                    // 成功接收到消息
                    let payload = msg.payload()
                        .ok_or_else(|| KafkaError::ReceiveFailed("Empty payload".to_string()))?;

                    // 反序列化
                    match serde_json::from_slice::<TokenEvent>(payload) {
                        Ok(event) => {
                            // 生成消息 ID: "partition:offset"
                            let msg_id = format!("{}:{}", msg.partition(), msg.offset());
                            events.push((msg_id, event));
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize TokenEvent from partition {} offset {}: {}",
                                msg.partition(),
                                msg.offset(),
                                e
                            );
                            // 反序列化失败的消息直接提交 offset 跳过
                            consumer.commit_message(&msg, CommitMode::Async)
                                .map_err(|e| KafkaError::ReceiveFailed(format!("Failed to commit message: {}", e)))?;
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("Kafka consumer error: {}", e);
                    return Err(KafkaError::ReceiveFailed(format!("Kafka consumer error: {}", e)).into());
                }
                Err(_) => {
                    // 超时，返回已收集的消息
                    break;
                }
            }
        }

        logger.log(&format!(
            "successfully received {} messages from kafka",
            events.len()
        ));
        Ok(events)
    }

    /// 确认消息已处理（提交 consumer offset）
    /// 对应 Redis 的 ack_messages
    /// todo!: 后续可能还是需要指定消费者
    // === 更改后 (ack_token_events) ===
    /// 确认 TokenEvent 消息已处理（提交指定 consumer 的 offset）
    pub async fn ack_token_events(&self, consumer_name: &str) -> Result<()> {
        let consumers = self.consumers.lock().await;

        // 只提交你真正想提交的那个 consumer
        if let Some(consumer) = consumers.get(consumer_name) {
            consumer.commit_consumer_state(CommitMode::Async)
                .map_err(|e| {
                    warn!("Failed to commit consumer '{}' state: {}", consumer_name, e);
                    KafkaError::ReceiveFailed(format!("Failed to commit consumer state: {}", e))
                })?;
            Ok(())
        } else {
            warn!("Attempted to ack non-existent consumer: {}", consumer_name);
            // 这是一个逻辑错误，但可能不是致命的，取决于你的业务
            Ok(())
        }
    }

    // ==================== Baseline Queue Methods ====================

    /// 批量发送 baseline 任务到 Kafka（优化版：并发发送）
    /// 对应 Redis 的 batch_enqueue_baseline_task
    pub async fn batch_enqueue_baseline_task(&self, mints: &[String]) -> Result<()> {
        if mints.is_empty() {
            return Ok(());
        }

        // 预先序列化所有消息（延长生命周期）
        let mut payloads = Vec::with_capacity(mints.len());
        let mut keys = Vec::with_capacity(mints.len());

        for mint in mints {
            // 简单的 JSON 格式：{"mint_pubkey": "..."}
            let payload = format!(r#"{{"mint_pubkey":"{}"}}"#, mint);
            let key = mint.clone();

            payloads.push(payload);
            keys.push(key);
        }

        // 并发发送所有消息（不等待）
        let mut send_futures = Vec::with_capacity(payloads.len());

        for (key, payload) in keys.iter().zip(payloads.iter()) {
            let record = FutureRecord::to(&self.config.baseline_topic)
                .key(key)
                .payload(payload);

            // 发送消息但不等待（返回 Future）
            let send_future = self.producer.send(record, Duration::from_secs(5));
            send_futures.push(send_future);
        }

        // 统一等待所有消息的发送结果
        let results = future::join_all(send_futures).await;

        // 检查是否有失败的发送
        for (idx, result) in results.into_iter().enumerate() {
            if let Err((e, _)) = result {
                warn!("Failed to send baseline task #{} to Kafka: {}", idx, e);
                return Err(KafkaError::SendFailed {
                    topic: self.config.baseline_topic.clone(),
                    reason: format!("Failed to send baseline task #{}: {}", idx, e),
                }.into());
            }
        }

        info!("✅ Successfully sent {} baseline tasks to Kafka", mints.len());
        Ok(())
    }

    /// 批量消费 baseline 任务从 Kafka（已优化：批量拉取）
    /// 对应 Redis 的 batch_dequeue_baseline_task
    pub async fn batch_dequeue_baseline_task(
        &self,
        consumer_name: &str,
        max_count: usize,
        block_ms: usize,
    ) -> Result<Vec<(String, String)>> {
        let start = Instant::now();

        // 获取或创建 consumer（已优化配置：批量拉取）
        let consumer = self
            .get_or_create_consumer(
                consumer_name,
                &self.config.baseline_topic,
                &self.config.baseline_consumer_group,
            )
            .await?;

        let mut baseline_tasks = Vec::with_capacity(max_count);
        let timeout = Duration::from_millis(block_ms as u64);
        let deadline = Instant::now() + timeout;

        while baseline_tasks.len() < max_count && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());

            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let payload = msg.payload()
                        .ok_or_else(|| KafkaError::ReceiveFailed("Empty payload".to_string()))?;

                    // 解析 JSON: {"mint_pubkey": "..."}
                    let payload_str = std::str::from_utf8(payload)
                        .map_err(|e| KafkaError::SerializationFailed(format!("Invalid UTF-8: {}", e)))?;

                    match serde_json::from_str::<serde_json::Value>(payload_str) {
                        Ok(json) => {
                            if let Some(mint_str) = json.get("mint_pubkey").and_then(|v| v.as_str())
                            {
                                let msg_id = format!("{}:{}", msg.partition(), msg.offset());
                                baseline_tasks.push((msg_id, mint_str.to_string()));
                            } else {
                                warn!("Message missing 'mint_pubkey' field, skipping");
                                consumer.commit_message(&msg, CommitMode::Async)
                                    .map_err(|e| KafkaError::ReceiveFailed(format!("Failed to commit message: {}", e)))?;
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse baseline task JSON: {}", e);
                            consumer.commit_message(&msg, CommitMode::Async)
                                .map_err(|e| KafkaError::ReceiveFailed(format!("Failed to commit message: {}", e)))?;
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("Kafka consumer error: {}", e);
                    return Err(KafkaError::ReceiveFailed(format!("Kafka consumer error: {}", e)).into());
                }
                Err(_) => {
                    // 超时，返回已收集的任务
                    break;
                }
            }
        }

        let elapsed = start.elapsed();
        if !baseline_tasks.is_empty() {
            info!(
                "✅ Consumed {} baseline tasks from Kafka in {:?}",
                baseline_tasks.len(),
                elapsed
            );
        }

        Ok(baseline_tasks)
    }

    /// 确认 Baseline 任务已处理（提交指定 consumer 的 offset）
    pub async fn ack_baseline_tasks(&self, consumer_name: &str) -> Result<()> {
        let consumers = self.consumers.lock().await;

        if let Some(consumer) = consumers.get(consumer_name) {
            consumer.commit_consumer_state(CommitMode::Async)
                .map_err(|e| {
                    warn!("Failed to commit baseline consumer '{}' state: {}", consumer_name, e);
                    KafkaError::ReceiveFailed(format!("Failed to commit consumer state: {}", e))
                })?;
            Ok(())
        } else {
            warn!("Attempted to ack non-existent consumer: {}", consumer_name);
            Ok(())
        }
    }
}
