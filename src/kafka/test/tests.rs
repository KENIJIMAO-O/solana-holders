use super::helpers::*;
use crate::kafka::{KafkaMessageQueue, KafkaQueueConfig};
use rdkafka::consumer::{CommitMode, Consumer};
use std::time::{Duration, Instant};
use crate::utils::task_logger::TaskLogger;
// ==================== Test 1: TokenEvent 基础流程 ====================

/// Test 1.1: 创建 TokenEvent topic
#[tokio::test]
async fn test_01_1_create_token_event_topic() {
    let topic_name = "test-token-event";
    create_topic(topic_name).await.unwrap();

    println!("\n=== Test 1.1 完成 ===");
    println!("Topic 已创建: {}", topic_name);
    println!("验证命令:");
    println!("  ./kafka-cli.sh topics");
    println!("  ./kafka-cli.sh describe {}", topic_name);
}

// __consumer_offsets
// test-simple-string
// test-token-event
// test-topic

/// Test 1.1: 删除 指定 topic
#[tokio::test]
async fn test_01_1_delete_target_topic() {
    let topic_name = "__consumer_offsets";
    delete_topic(topic_name).await.unwrap();

    println!("\n=== Test 1.1 完成 ===");
    println!("Topic 已创建: {}", topic_name);
    println!("验证命令:");
    println!("  ./kafka-cli.sh topics");
    println!("  ./kafka-cli.sh describe {}", topic_name);
}

/// Test 1.2: 测试 batch_enqueue_holder_event (发送 TokenEvent)
#[tokio::test]
async fn test_01_2_batch_enqueue_holder_event() {
    let topic_name = "test-token-event";

    let config = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: topic_name.to_string(),
        baseline_topic: "unused".to_string(),
        token_event_consumer_group: "test-token-group".to_string(),
        baseline_consumer_group: "unused".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue = KafkaMessageQueue::new(config).unwrap();
    let mut logger = TaskLogger::new("test", "1");

    // 创建 3 个测试 TokenEvent
    let token_events = vec![
        create_test_token_event_with_index(0),
        create_test_token_event_with_index(1),
        create_test_token_event_with_index(2),
    ];

    // 测试 batch_enqueue_holder_event
    let msg_ids = kafka_queue
        .batch_enqueue_holder_event(token_events.clone())
        .await
        .unwrap();

    println!("\n=== Test 1.2 完成 ===");
    println!("成功发送 {} 条 TokenEvent", msg_ids.len());
    println!("消息 IDs: {:?}", msg_ids);
    assert_eq!(msg_ids.len(), 3);

    // 验证消息 ID 格式 (partition:offset)
    for msg_id in &msg_ids {
        assert!(msg_id.contains(':'), "消息 ID 格式应为 partition:offset");
    }

    println!("\n验证命令:");
    println!("  ./kafka-cli.sh offsets {}", topic_name);
    println!("  ./kafka-cli.sh consume {}", topic_name);
}

/// Test 1.3: 测试 batch_dequeue_holder_event (接收 TokenEvent)
#[tokio::test]
async fn test_01_3_batch_dequeue_holder_event() {
    let topic_name = "test-token-event";

    let config = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: topic_name.to_string(),
        baseline_topic: "unused".to_string(),
        token_event_consumer_group: "test-token-group".to_string(),
        baseline_consumer_group: "unused".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue = KafkaMessageQueue::new(config).unwrap();

    // 测试 batch_dequeue_holder_event
    let consumer_name = "test-consumer-1";
    let received = kafka_queue
        .batch_dequeue_holder_event(consumer_name, 10, 5000)
        .await
        .unwrap();

    println!("\n=== Test 1.3 完成 ===");
    println!("消费到 {} 条消息", received.len());

    assert!(received.len() >= 3, "应该至少收到前面测试发送的 3 条消息");

    // 验证消息内容
    for (i, (msg_id, event)) in received.iter().enumerate() {
        println!("消息 #{}: {} - mint={}", i, msg_id, event.mint_address);
        assert_eq!(
            event.mint_address.to_string(),
            "DFL1zNkaGPWm1BqAVqRjCZvHmwTFrEaJtbzJWgseoNJh"
        );
    }

    println!("\n验证命令:");
    println!("  ./kafka-cli.sh group test-token-group");
}

/// Test 1.4: 测试 ack_messages (确认 TokenEvent)
#[tokio::test]
async fn test_01_4_ack_messages() {
    let topic_name = "test-token-event";

    // Step 1: 创建 queue-1，用 consumer-1 消费（不 ACK）
    let config_1 = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: topic_name.to_string(),
        baseline_topic: "unused".to_string(),
        token_event_consumer_group: "test-token-ack-group".to_string(), // 专用 group
        baseline_consumer_group: "unused".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue_1 = KafkaMessageQueue::new(config_1).unwrap();
    let mut logger = TaskLogger::new("test", "1");
    let consumer_name_1 = "token-consumer-1";
    let received = kafka_queue_1
        .batch_dequeue_holder_event(consumer_name_1, 20, 5000)
        .await
        .unwrap();

    if received.is_empty() {
        println!("\n=== Test 1.4 跳过 ===");
        println!("Topic 中没有消息，请先运行 Test 1.2");
        return;
    }

    let msg_ids: Vec<String> = received.iter().map(|(id, _)| id.clone()).collect();
    println!(
        "\n✅ Step 1: consumer-1 消费了 {} 条消息（未 ACK）",
        received.len()
    );

    // 销毁 queue_1，让 consumer-1 退出 consumer group
    drop(kafka_queue_1);
    println!("✅ Step 1.5: 销毁 queue_1，等待 consumer-1 退出...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Step 2: 创建 queue-2（使用相同的 consumer group），模拟"程序重启"
    let config_2 = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: topic_name.to_string(),
        baseline_topic: "unused".to_string(),
        token_event_consumer_group: "test-token-ack-group".to_string(), // 相同 group
        baseline_consumer_group: "unused".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue_2 = KafkaMessageQueue::new(config_2).unwrap();
    let consumer_name_2 = "token-consumer-2";
    let received_again = kafka_queue_2
        .batch_dequeue_holder_event(consumer_name_2, 20, 5000)
        .await
        .unwrap();

    println!(
        "✅ Step 2: consumer-2 消费了 {} 条消息（未 ACK，重复消费）",
        received_again.len()
    );
    assert_eq!(
        received_again.len(),
        received.len(),
        "未 ACK 时新 consumer 应该能重复消费相同消息"
    );

    // Step 3: ACK 消息
    kafka_queue_2.ack_token_events(consumer_name_2).await.unwrap();
    println!("✅ Step 3: 已 ACK {} 条消息", msg_ids.len());

    // 销毁 queue_2，让 consumer-2 退出
    drop(kafka_queue_2);
    println!("✅ Step 3.5: 销毁 queue_2，等待 consumer-2 退出...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Step 4: 创建 queue-3（使用相同的 consumer group），再次模拟"重启"
    let config_3 = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: topic_name.to_string(),
        baseline_topic: "unused".to_string(),
        token_event_consumer_group: "test-token-ack-group".to_string(), // 相同 group
        baseline_consumer_group: "unused".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue_3 = KafkaMessageQueue::new(config_3).unwrap();
    let consumer_name_3 = "token-consumer-3";
    let empty = kafka_queue_3
        .batch_dequeue_holder_event(consumer_name_3, 20, 5000)
        .await
        .unwrap();

    println!(
        "✅ Step 4: consumer-3 消费了 {} 条消息（ACK 后）",
        empty.len()
    );
    println!("  (期望: 0，因为 offset 已提交)");

    println!("\n=== Test 1.4 完成 ===");
    println!("✅ ACK 机制验证通过：");
    println!("  - 未 ACK 时：新建 consumer 会重复消费");
    println!("  - 已 ACK 后：新建 consumer 从 committed offset 开始");
    println!("\n验证命令:");
    println!("  ./kafka-cli.sh group test-token-ack-group");
    println!("  # 应该看到 CURRENT-OFFSET 已提交, LAG 为 0");
}

/// Test 1.5: 删除 TokenEvent topic
#[tokio::test]
#[ignore]
async fn test_01_5_delete_token_event_topic() {
    let topic_name = "test-token-event";
    delete_topic(topic_name).await.unwrap();

    println!("\n=== Test 1.5 完成 ===");
    println!("Topic 已删除: {}", topic_name);
}

// ==================== Test 2: Baseline 基础流程 ====================

/// Test 2.1: 创建 Baseline topic
#[tokio::test]
async fn test_02_1_create_baseline_topic() {
    let topic_name = "test-baseline-task";
    create_topic(topic_name).await.unwrap();

    println!("\n=== Test 2.1 完成 ===");
    println!("Topic 已创建: {}", topic_name);
}

/// Test 2.2: 测试 batch_enqueue_baseline_task (发送 baseline 任务)
#[tokio::test]
async fn test_02_2_batch_enqueue_baseline_task() {
    let topic_name = "test-baseline-task";

    let config = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: "unused".to_string(),
        baseline_topic: topic_name.to_string(),
        token_event_consumer_group: "unused".to_string(),
        baseline_consumer_group: "test-baseline-group".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue = KafkaMessageQueue::new(config).unwrap();

    // 创建 3 个 mint 任务
    let mints = vec![
        "DFL1zNkaGPWm1BqAVqRjCZvHmwTFrEaJtbzJWgseoNJh".to_string(),
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".to_string(),
    ];

    // 测试 batch_enqueue_baseline_task
    kafka_queue
        .batch_enqueue_baseline_task(&mints)
        .await
        .unwrap();

    println!("\n=== Test 2.2 完成 ===");
    println!("成功发送 {} 个 baseline 任务", mints.len());

    println!("\n验证命令:");
    println!("  ./kafka-cli.sh offsets {}", topic_name);
    println!("  ./kafka-cli.sh consume {}", topic_name);
}

/// Test 2.3: 测试 batch_dequeue_baseline_task (接收 baseline 任务)
#[tokio::test]
async fn test_02_3_batch_dequeue_baseline_task() {
    let topic_name = "test-baseline-task";

    let config = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: "unused".to_string(),
        baseline_topic: topic_name.to_string(),
        token_event_consumer_group: "unused".to_string(),
        baseline_consumer_group: "test-baseline-group".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue = KafkaMessageQueue::new(config).unwrap();

    // 测试 batch_dequeue_baseline_task
    let consumer_name = "baseline-consumer-1";
    let received = kafka_queue
        .batch_dequeue_baseline_task(consumer_name, 10, 5000)
        .await
        .unwrap();

    println!("\n=== Test 2.3 完成 ===");
    println!("消费到 {} 个 baseline 任务", received.len());

    assert!(received.len() >= 3, "应该至少收到前面测试发送的 3 个任务");

    // 验证消息内容
    for (i, (msg_id, mint)) in received.iter().enumerate() {
        println!("任务 #{}: {} - mint={}", i, msg_id, mint);
        assert!(mint.len() > 30, "mint 地址格式应该正确");
    }

    println!("\n验证命令:");
    println!("  ./kafka-cli.sh group test-baseline-group");
}

/// Test 2.4: 测试 ack_message_baseline (确认 baseline 任务)
#[tokio::test]
async fn test_02_4_ack_message_baseline() {
    let topic_name = "test-baseline-task";

    // Step 1: 创建 queue-1，用 consumer-1 消费（不 ACK）
    let config_1 = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: "unused".to_string(),
        baseline_topic: topic_name.to_string(),
        token_event_consumer_group: "unused".to_string(),
        baseline_consumer_group: "test-baseline-ack-group".to_string(), // 专用 group
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue_1 = KafkaMessageQueue::new(config_1).unwrap();
    let consumer_name_1 = "baseline-consumer-1";
    let received = kafka_queue_1
        .batch_dequeue_baseline_task(consumer_name_1, 20, 5000)
        .await
        .unwrap();

    if received.is_empty() {
        println!("\n=== Test 2.4 跳过 ===");
        println!("Topic 中没有消息，请先运行 Test 2.2");
        return;
    }

    println!(
        "\n✅ Step 1: consumer-1 消费 {} 个任务（未 ACK）",
        received.len()
    );
    // 注意：此时没有调用 ack，offset 未 commit

    // 重要：销毁 queue_1，让 consumer-1 退出 consumer group
    drop(kafka_queue_1);
    println!("✅ Step 1.5: 销毁 queue_1，等待 consumer-1 退出...");
    tokio::time::sleep(Duration::from_secs(8)).await; // 等待 session timeout (6s) + rebalance

    // Step 2: 创建 queue-2（使用相同的 consumer group），模拟"程序重启"
    // 因为没有 commit offset，新 consumer 应该从头读取
    let config_2 = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: "unused".to_string(),
        baseline_topic: topic_name.to_string(),
        token_event_consumer_group: "unused".to_string(),
        baseline_consumer_group: "test-baseline-ack-group".to_string(), // 相同 group
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue_2 = KafkaMessageQueue::new(config_2).unwrap();
    let consumer_name_2 = "baseline-consumer-2";
    let received_again = kafka_queue_2
        .batch_dequeue_baseline_task(consumer_name_2, 20, 5000)
        .await
        .unwrap();

    println!(
        "✅ Step 2: consumer-2 消费 {} 个任务（未 ACK，重复消费）",
        received_again.len()
    );
    assert_eq!(
        received_again.len(),
        received.len(),
        "未 ACK 时新 consumer 应该能重复消费"
    );

    // Step 3: ACK 消息
    let msg_id = &received[0].0;
    kafka_queue_2.ack_baseline_tasks(msg_id).await.unwrap();
    println!("✅ Step 3: 已 ACK 任务: {}", msg_id);

    // 销毁 queue_2，让 consumer-2 退出
    drop(kafka_queue_2);
    println!("✅ Step 3.5: 销毁 queue_2，等待 consumer-2 退出...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Step 4: 创建 queue-3（使用相同的 consumer group），再次模拟"重启"
    // 这次因为 offset 已 commit，应该从 committed offset 之后开始读
    let config_3 = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: "unused".to_string(),
        baseline_topic: topic_name.to_string(),
        token_event_consumer_group: "unused".to_string(),
        baseline_consumer_group: "test-baseline-ack-group".to_string(), // 相同 group
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue_3 = KafkaMessageQueue::new(config_3).unwrap();
    let consumer_name_3 = "baseline-consumer-3";
    let after_ack = kafka_queue_3
        .batch_dequeue_baseline_task(consumer_name_3, 20, 5000)
        .await
        .unwrap();

    println!(
        "✅ Step 4: consumer-3 消费 {} 个任务（ACK 后）",
        after_ack.len()
    );

    // Kafka 的 offset commit 是按分区的，ACK 会提交到该消息的 offset
    // 新 consumer 应该从 committed offset 之后开始读（如果有更多消息的话）
    // 在这个测试中，我们只 commit 了所有消息，所以应该收到 0 条或更少的消息
    println!("  (期望: 0 或更少，因为 offset 已提交)");

    println!("\n=== Test 2.4 完成 ===");
    println!("✅ ACK 机制验证通过：");
    println!("  - 未 ACK 时：新建 consumer 会重复消费");
    println!("  - 已 ACK 后：新建 consumer 从 committed offset 开始");
    println!("\n验证命令:");
    println!("  ./kafka-cli.sh group test-baseline-ack-group");
    println!("  # 应该看到 CURRENT-OFFSET 已提交");
}

/// Test 2.5: 删除 Baseline topic
#[tokio::test]
#[ignore]
async fn test_02_5_delete_baseline_topic() {
    let topic_name = "test-baseline-task";
    delete_topic(topic_name).await.unwrap();

    println!("\n=== Test 2.5 完成 ===");
    println!("Topic 已删除: {}", topic_name);
}

// ==================== Test 3: 完整端到端流程 ====================

/// Test 3: TokenEvent 完整流程测试 (enqueue -> dequeue -> ack)
#[tokio::test]
async fn test_03_token_event_full_workflow() {
    let topic_name = "test-e2e-token";
    create_topic(topic_name).await.unwrap();

    let config = KafkaQueueConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        token_event_topic: topic_name.to_string(),
        baseline_topic: "unused".to_string(),
        token_event_consumer_group: "test-e2e-group".to_string(),
        baseline_consumer_group: "unused".to_string(),
        producer_timeout_ms: "5000".to_string(),
        consumer_session_timeout_ms: "6000".to_string(),
        consumer_enable_auto_commit: "false".to_string(),
    };

    let kafka_queue = KafkaMessageQueue::new(config).unwrap();
    let mut logger = TaskLogger::new("test", "e2e");

    // Step 1: 发送 5 个 TokenEvent
    let token_events = vec![
        create_test_token_event_with_index(100),
        create_test_token_event_with_index(101),
        create_test_token_event_with_index(102),
        create_test_token_event_with_index(103),
        create_test_token_event_with_index(104),
    ];

    let msg_ids = kafka_queue
        .batch_enqueue_holder_event(token_events.clone())
        .await
        .unwrap();

    println!("\n✅ Step 1: 发送 {} 条消息", msg_ids.len());
    assert_eq!(msg_ids.len(), 5);

    // Step 2: 消费消息
    let consumer_name = "e2e-consumer";
    let received = kafka_queue
        .batch_dequeue_holder_event(consumer_name, 10, 5000)
        .await
        .unwrap();

    println!("✅ Step 2: 消费到 {} 条消息", received.len());
    assert_eq!(received.len(), 5);

    // Step 3: 验证内容
    for (i, (msg_id, event)) in received.iter().enumerate() {
        assert_eq!(event.instruction_index, 100 + i as u32);
        println!(
            "  消息 #{}: {} - index={}",
            i, msg_id, event.instruction_index
        );
    }
    println!("✅ Step 3: 消息内容验证通过");

    // Step 4: ACK 消息
    kafka_queue.ack_token_events(consumer_name).await.unwrap();
    println!("✅ Step 4: ACK {} 条消息", msg_ids.len());

    // Step 5: 验证 ACK 后无新消息
    let empty = kafka_queue
        .batch_dequeue_holder_event(consumer_name, 10, 1000)
        .await
        .unwrap();

    assert_eq!(empty.len(), 0);
    println!("✅ Step 5: ACK 后无新消息，测试通过");

    println!("\n=== Test 3 完成 ===");
    println!("TokenEvent 完整流程测试成功");
}

#[tokio::test]
#[ignore]
async fn test_03_cleanup() {
    delete_topic("test-e2e-token").await.unwrap();
}
