use anyhow::anyhow;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, future_producer::Delivery};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::time::Duration;

use crate::monitor::monitor::TokenEvent;

/// 创建 Kafka topic
pub async fn create_topic(topic_name: &str) -> anyhow::Result<()> {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()?;

    let topics = &[NewTopic::new(
        topic_name,
        1,                          // 1 个分区
        TopicReplication::Fixed(1), // 副本因子 1
    )];

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    let results = admin.create_topics(topics, &opts).await?;

    for result in results {
        match result {
            Ok(topic) => println!("✅ Created topic: {}", topic),
            Err((topic, err)) => {
                if err.to_string().contains("already exists") {
                    println!("ℹ️  Topic {} already exists", topic);
                } else {
                    return Err(anyhow!("Failed to create topic {}: {}", topic, err));
                }
            }
        }
    }

    Ok(())
}

/// 删除 Kafka topic
pub async fn delete_topic(topic_name: &str) -> anyhow::Result<()> {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()?;

    let topics = &[topic_name];
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    let results = admin.delete_topics(topics, &opts).await?;

    for result in results {
        match result {
            Ok(topic) => println!("✅ Deleted topic: {}", topic),
            Err((topic, err)) => {
                println!("⚠️  Failed to delete topic {}: {}", topic, err);
            }
        }
    }

    Ok(())
}

/// 创建测试用的 Producer
pub fn create_test_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create producer")
}

/// 创建测试用的 Consumer
pub fn create_test_consumer(group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("Failed to create consumer")
}

/// 发送简单字符串消息
pub async fn send_string(
    producer: &FutureProducer,
    topic: &str,
    payload: &str,
) -> anyhow::Result<Delivery> {
    let record = FutureRecord::to(topic).key("test-key").payload(payload);

    match producer.send(record, Duration::from_secs(5)).await {
        Ok(delivery) => Ok(delivery),
        Err((e, _)) => Err(anyhow!("Failed to send: {}", e)),
    }
}

/// 创建带索引的测试 TokenEvent
pub fn create_test_token_event_with_index(index: u32) -> TokenEvent {
    let mint = Pubkey::from_str("DFL1zNkaGPWm1BqAVqRjCZvHmwTFrEaJtbzJWgseoNJh").unwrap();
    let account = Pubkey::from_str("11111111111111111111111111111111").unwrap();

    TokenEvent {
        slot: 12345 + index as u64,
        tx_signature: format!("test_sig_{}", index),
        instruction_index: index,
        mint_address: mint,
        account_address: account,
        owner_address: Some(account),
        delta: "1000000".to_string(),
        confirmed: true,
    }
}

/// 获取已提交的 offset
pub async fn get_committed_offset(consumer: &StreamConsumer, topic: &str, partition: i32) -> i64 {
    use rdkafka::TopicPartitionList;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);

    match consumer.committed_offsets(tpl, Duration::from_secs(5)) {
        Ok(offsets) => offsets
            .find_partition(topic, partition)
            .and_then(|p| p.offset().to_raw())
            .unwrap_or(-1),
        Err(e) => {
            println!("Failed to get committed offset: {}", e);
            -1
        }
    }
}

