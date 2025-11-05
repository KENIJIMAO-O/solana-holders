pub(crate) use crate::message_queue::{Redis, RedisQueueConfig};
use crate::monitor::monitor::{InstructionType, TokenEvent};
use crate::utils::timer::TaskLogger;
use anyhow::{Error, Result, anyhow};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Value};
use tracing::{info, warn};

impl Redis {
    /// 初始化消息队列
    pub async fn initialize_message_queue(&self) -> Result<(), Error> {
        let mut conn = self.get_connection().await?;
        let config = self.redis_queue_config.clone();
        let stream_name = config.token_event_namespace;

        // 尝试创建消费者组，如果stream已存在则忽略错误，否则同时创建stream
        // stream_name 为stream键名
        // consumer_group 要创建的消费组的名称
        match conn
            .xgroup_create_mkstream::<_, _, _, String>(
                &stream_name,
                &config.token_event_consumer_group,
                "0",
            )
            .await
        {
            Ok(result) => {
                info!(
                    "✅ [REDIS] Consumer group '{}' created successfully for stream '{}': {}",
                    config.token_event_consumer_group, stream_name, result
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("BUSYGROUP") {
                    // 启动时，消息队列已经存在，属于正常情况
                    info!(
                        "ℹ️ [REDIS] Consumer group '{}' already exists for stream '{}' - continuing",
                        config.token_event_consumer_group, stream_name
                    );
                } else {
                    // 其他错误需要报告
                    warn!(
                        "⚠️ [REDIS] Failed to create consumer group '{}' for stream '{}': {}",
                        config.token_event_consumer_group, stream_name, error_msg
                    );
                    return Err(Error::from(e));
                }
            }
        }

        info!("✅ [REDIS] Token event queue initialization completed successfully");

        Ok(())
    }

    pub async fn batch_enqueue_holder_event(
        &self,
        token_events: Vec<TokenEvent>,
        monitor_logger: &mut TaskLogger,
    ) -> Result<Vec<String>, Error> {
        if token_events.is_empty() {
            return Err(anyhow!("empty token event"));
        };

        monitor_logger.log("start to getting a connection");
        let mut conn = self.get_connection().await?;
        let stream_name = &self.redis_queue_config.token_event_namespace;

        let mut pipe = redis::pipe();
        monitor_logger.log("start to push stream into pipe");
        for token_event in token_events.clone() {
            // stream_name，即流的键名
            // "*"，告诉redis请自动帮我生成唯一的、有序的ID
            pipe.xadd(
                stream_name,
                "*",
                &[
                    ("slot", token_event.slot.to_string()),
                    ("tx_sig", token_event.tx_signature),
                    ("ix_idx", token_event.instruction_index.to_string()),
                    ("mint", token_event.mint_address.to_string()),
                    ("account", token_event.account_address.to_string()),
                    (
                        "owner",
                        token_event
                            .owner_address
                            .map(|o| o.to_string())
                            .unwrap_or_default(),
                    ),
                    ("delta", token_event.delta),
                    ("ix_type", format!("{:?}", token_event.instruction_type)),
                    ("confirmed", token_event.confirmed.to_string()),
                ],
            );
        }

        let results: Vec<String> = pipe.query_async(&mut conn).await?;

        monitor_logger.log("start to batch renew stats");
        // 批量更新统计
        let stats_key = format!("{}:stats:enqueued", stream_name);
        let _: i32 = conn.incr(&stats_key, token_events.len() as i32).await?;
        if let Some(stats_ttl) = self.redis_queue_config.stats_ttl {
            let _: () = conn.expire(&stats_key, stats_ttl).await?;
        }

        Ok(results)
    }

    /// 批量出队
    pub async fn batch_dequeue_holder_event(
        &self,
        consumer_name: &str,
        max_count: usize,
        block_ms: usize,
        logger: &mut TaskLogger,
    ) -> Result<Vec<(String, TokenEvent)>, Error> {
        logger.debug("start to getting connection");
        let mut conn = self.get_connection().await?;
        let stream_name = &self.redis_queue_config.token_event_namespace;
        let consumer_group = &self.redis_queue_config.token_event_consumer_group;

        logger.debug("start to batch reading");

        let opts = StreamReadOptions::default()
            .group(&consumer_group, consumer_name)
            .count(max_count)
            .block(block_ms);
        let results: StreamReadReply = conn.xread_options(&[stream_name], &[">"], &opts).await?;
        let mut events = Vec::new();

        logger.debug("start to parsing data");
        // 解析 Redis Stream 返回的数据
        for stream_key in results.keys {
            for stream_id in stream_key.ids {
                let message_id = stream_id.id.clone();

                // 从 Redis 数据解析为 TokenEvent
                match Self::parse_token_event_from_redis(&stream_id.map) {
                    Ok(token_event) => {
                        events.push((message_id, token_event));
                    }
                    Err(e) => {
                        warn!(
                            "⚠️ [REDIS] Failed to parse TokenEvent from message {}: {}",
                            message_id, e
                        );
                        // 解析失败的消息也要 ACK，避免重复处理
                        let _ = self.ack_message_token_event(&message_id).await;
                    }
                }
            }
        }

        logger.debug("start to renewing statistic");
        // 更新统计
        if !events.is_empty() {
            let stats_key = format!("{}:stats:dequeued", stream_name);
            let _: i32 = conn.incr(&stats_key, events.len() as i32).await?;
            if let Some(stats_ttl) = self.redis_queue_config.stats_ttl {
                let _: () = conn.expire(&stats_key, stats_ttl).await?;
            }
        }

        Ok(events)
    }

    /// 确认消息已处理（ACK）
    pub async fn ack_message_token_event(&self, message_id: &str) -> Result<(), Error> {
        let mut conn = self.get_connection().await?;
        let stream_name = &self.redis_queue_config.token_event_namespace;
        let consumer_group = &self.redis_queue_config.token_event_consumer_group;

        let _: i32 = conn
            .xack(stream_name, consumer_group, &[message_id])
            .await
            .map_err(|e| anyhow!("Failed to ACK message {}: {}", message_id, e))?;

        Ok(())
    }

    fn parse_token_event_from_redis(
        map: &std::collections::HashMap<String, Value>,
    ) -> Result<TokenEvent, Error> {
        use redis::FromRedisValue;
        use solana_sdk::pubkey::Pubkey;
        use std::str::FromStr; // 引入这个 Trait

        // 辅助函数：从 map 中获取值并解析为目标类型 T
        let get_and_parse = |key: &str| -> Result<String, Error> {
            let value = map
                .get(key)
                .ok_or_else(|| anyhow!("Missing field: {}", key))?;
            String::from_redis_value(value)
                .map_err(|e| anyhow!("Failed to parse {} as String: {}", key, e))
        };

        // 解析各个字段
        let slot = get_and_parse("slot")?.parse::<i64>()?;
        let tx_signature = get_and_parse("tx_sig")?;
        let instruction_index = get_and_parse("ix_idx")?.parse::<u32>()?;

        let mint_address = Pubkey::from_str(&get_and_parse("mint")?)?;
        let account_address = Pubkey::from_str(&get_and_parse("account")?)?;

        let owner_str = get_and_parse("owner")?;
        let owner_address = if owner_str.is_empty() {
            None
        } else {
            Some(Pubkey::from_str(&owner_str)?)
        };

        let delta = get_and_parse("delta")?;

        let ix_type_str = get_and_parse("ix_type")?;
        // let instruction_type = Self::parse_instruction_type(&ix_type_str)?;
        let instruction_type = InstructionType::Other;

        let confirmed = get_and_parse("confirmed")?.parse::<bool>()?;

        Ok(TokenEvent {
            slot,
            tx_signature,
            instruction_index,
            mint_address,
            account_address,
            owner_address,
            delta,
            instruction_type,
            confirmed,
        })
    }

    /// 解析 InstructionType（简化实现）
    fn parse_instruction_type(s: &str) -> Result<InstructionType, Error> {
        // 这里需要根据你的 Debug 格式来解析
        // 简化版本，只处理基本类型
        if s.starts_with("Transfer") {
            // TODO: 解析完整的 Transfer 参数
            Ok(InstructionType::Other) // 临时返回 Other
        } else if s.starts_with("TransferChecked") {
            Ok(InstructionType::Other)
        } else if s.starts_with("MintTo") {
            Ok(InstructionType::MintTo())
        } else if s.starts_with("Burn") {
            Ok(InstructionType::Burn())
        } else {
            Ok(InstructionType::Other)
        }
    }

    /// 批量确认消息
    pub async fn ack_messages(&self, message_ids: &[String]) -> Result<(), Error> {
        if message_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;
        let stream_name = &self.redis_queue_config.token_event_namespace;
        let consumer_group = &self.redis_queue_config.token_event_consumer_group;

        let ids: Vec<&str> = message_ids.iter().map(|s| s.as_str()).collect();
        let _: i32 = conn
            .xack(stream_name, consumer_group, &ids)
            .await
            .map_err(|e| anyhow!("Failed to ACK messages: {}", e))?;

        Ok(())
    }

    // todo!: 缺少清理PEL函数
}

#[cfg(test)]
mod tests {
    use super::*;

    pub async fn new_redis() -> Redis {
        dotenv::dotenv().ok();
        let config = RedisQueueConfig::default();
        let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");
        let redis = Redis::new(&redis_url, config).await.unwrap();
        redis
    }
    #[tokio::test]
    async fn test_redis_initialize() {
        let redis = new_redis().await;
        let res = match redis.initialize_message_queue().await {
            Ok(_) => {
                println!("Successfully initialized message queue");
            }
            Err(e) => panic!("failed initialized message queue, cause:{:?}", e),
        };
    }
}
