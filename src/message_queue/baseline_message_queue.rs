use crate::message_queue::Redis;
use anyhow::{Error, anyhow};
use redis::AsyncCommands;
use redis::streams::{StreamReadOptions, StreamReadReply};
use tracing::{info, warn};

impl Redis {
    /// 初始化消息队列
    pub async fn init_baseline_queue(&self) -> anyhow::Result<(), Error> {
        let mut conn = self.get_connection().await?;
        let config = self.redis_queue_config.clone();
        let stream_name = config.baseline_namespace;

        match conn
            .xgroup_create_mkstream::<_, _, _, String>(
                &stream_name,
                &config.baseline_consumer_group,
                "0",
            )
            .await
        {
            Ok(result) => {
                info!(
                    "✅ [REDIS] Consumer group '{}' created successfully for stream '{}': {}",
                    config.baseline_consumer_group, stream_name, result
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("BUSYGROUP") {
                    info!(
                        "Consumer group '{}' already exists for stream '{}' - continuing",
                        config.baseline_consumer_group, stream_name
                    );
                } else {
                    warn!(
                        "Failed to create consumer group '{}' for stream '{}': {}",
                        config.baseline_consumer_group, stream_name, error_msg
                    );
                    return Err(Error::from(e));
                }
            }
        };

        info!("✅ [REDIS] Baseline queue initialization completed successfully");
        Ok(())
    }

    pub async fn batch_enqueue_baseline_task(&self, mints: &[String]) -> Result<(), Error> {
        if mints.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;
        let stream_name = &self.redis_queue_config.baseline_namespace;

        let mut pipe = redis::pipe();

        for mint in mints {
            pipe.xadd(stream_name, "*", &[("mint_pubkey", mint)]);
        }
        let _result: Vec<String> = pipe.query_async(&mut conn).await?;

        // 更新统计
        let stats_key = format!("{}:stats:enqueued", stream_name); // 入队数据统计的key
        let _: i32 = conn.incr(&stats_key, mints.len() as i32).await?; // incr方法：将一个key值对应的值增加指定数字，这里是mints.len()
        if let Some(stats_ttl) = self.redis_queue_config.stats_ttl {
            let _: () = conn.expire(&stats_key, stats_ttl).await?; // 为数据统计key设置ttl
        }
        Ok(())
    }

    // 批量出队baseline task，不过到后期可能每次只能出队一个，到时候需要根据空闲连接数来动态设置
    // 返回 (message_id, mint) 元组，由调用者在处理成功后 ACK
    pub async fn batch_dequeue_baseline_task(
        &self,
        consumer_name: &str,
        max_count: usize,
        block_ms: usize,
    ) -> Result<Vec<(String, String)>, Error> {
        // 1. 获取 Redis 连接
        let mut conn = self
            .blocking_queue_client
            .get_multiplexed_async_connection()
            .await?;
        let stream_name = &self.redis_queue_config.baseline_namespace;
        let consumer_group = &self.redis_queue_config.baseline_consumer_group;

        // 2. 设置读取选项并从 Stream 中批量拉取消息
        let opts = StreamReadOptions::default()
            .group(consumer_group, consumer_name)
            .count(max_count)
            .block(block_ms);
        let results: StreamReadReply = conn.xread_options(&[stream_name], &[">"], &opts).await?;

        let mut baseline_tasks = Vec::with_capacity(max_count);

        // 3. 遍历并解析返回的消息
        for stream_key in results.keys {
            for stream_id in stream_key.ids {
                let message_id = stream_id.id;

                // 从消息体中获取 "mint_pubkey" 字段的值
                let mint_value = match stream_id.map.get("mint_pubkey") {
                    Some(value) => value,
                    None => {
                        // 如果消息没有 "mint_pubkey" 字段，说明是无效消息
                        warn!(
                            "Message {} is missing 'mint_pubkey' field. Acknowledging and skipping.",
                            message_id
                        );
                        // 无效消息立即 ACK，避免阻塞队列
                        let _ = self.ack_message_baseline(&message_id).await;
                        continue; // 继续处理下一条消息
                    }
                };

                // 将 Redis 值解析为 String
                match redis::from_redis_value::<String>(mint_value) {
                    Ok(mint_str) => {
                        // 解析成功，返回 (message_id, mint) 元组
                        baseline_tasks.push((message_id, mint_str));
                    }
                    Err(e) => {
                        // 如果值无法解析为 String，同样是无效消息
                        warn!(
                            "Failed to parse mint from message {}: {}. Acknowledging and skipping.",
                            message_id, e
                        );
                        // 无效消息立即 ACK，避免阻塞队列
                        let _ = self.ack_message_baseline(&message_id).await;
                        continue; // 继续处理下一条消息
                    }
                }
            }
        }

        // 4. 更新出队统计信息
        if !baseline_tasks.is_empty() {
            let stats_key = format!("{}:stats:dequeued", stream_name);
            let _: i32 = conn.incr(&stats_key, baseline_tasks.len() as i32).await?;
            if let Some(stats_ttl) = self.redis_queue_config.stats_ttl {
                let _: () = conn.expire(&stats_key, stats_ttl).await?;
            }
        }

        Ok(baseline_tasks)
    }

    pub async fn ack_message_baseline(&self, message_id: &str) -> Result<(), Error> {
        let mut conn = self.get_connection().await?;
        let stream_name = &self.redis_queue_config.baseline_namespace;
        let consumer_group = &self.redis_queue_config.baseline_consumer_group;

        let _: i32 = conn
            .xack(stream_name, consumer_group, &[message_id])
            .await
            .map_err(|e| anyhow!("Failed to ACK message {} : {}", message_id, e))?;

        Ok(())
    }
}
