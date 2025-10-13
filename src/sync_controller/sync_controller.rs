use crate::baseline::getProgramAccounts::HttpClient;
use crate::database::postgresql::DatabaseConnection;
use crate::message_queue::message_queue::Redis;
use crate::repositories::events::{Event, EventsRepository};
use crate::repositories::holders::HoldersRepository;
use crate::repositories::mint_stats::MintStatsRepository;
use crate::repositories::token_accounts::TokenAccountsRepository;
use crate::utils::timer::TaskLogger;
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Clone)]
pub struct SyncController {
    pub redis: Arc<Redis>,
    pub database: Arc<DatabaseConnection>,
    pub http_client: Arc<HttpClient>,
}

/// todo!: 还有一比较关键的东西没做，就是需要判断一下消息队列的发送端和接收端处理的时候是否需要用到锁
impl SyncController {
    pub fn new(
        redis: Arc<Redis>,
        database: Arc<DatabaseConnection>,
        http_client: Arc<HttpClient>,
    ) -> Self {
        Self {
            redis,
            database,
            http_client,
        }
    }

    pub async fn consume_events_from_queue(
        &self,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 1000;
        const BATCH_TIMEOUT_MS: u64 = 100; // Duration::from_millis 接收 u64
        let consumer_name = "sync_dequeuer";

        loop {
            let mut logger = TaskLogger::new("sync_controller", "2");

            // 使用 tokio::select! 来并发地监听“取消信号”和“出队操作”
            tokio::select! {
                // 分支 1: 如果收到了取消信号
                _ = cancellation_token.cancelled() => {
                    info!("Monitor received cancellation signal. Shutting down...");
                    break; // 跳出 loop 循环，函数将优雅地结束
                }

                // todo!: 这里要干的一件事情就是将新出现的token，进行baseline构建，所以需要一张新表

                // 分支 2: 执行出队操作
                datas_result = self.redis.batch_dequeue_holder_event(
                    consumer_name,
                    BATCH_SIZE,
                    BATCH_TIMEOUT_MS as usize, // 确保类型匹配
                    &mut logger,
                ) => {
                    let datas = match datas_result {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Failed to dequeue from Redis: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue; // 继续下一次循环
                        }
                    };

                    if datas.is_empty() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }

                    logger.log("batch dequeue holder events complete");

                    // --- 数据清洗与转换 ---
                    // 使用 filter_map 来安全地解析和过滤数据
                    let (message_ids, token_events): (Vec<String>, Vec<Event>) = datas
                        .into_iter()
                        .filter_map(|(message_id, raw_event)| {
                            // 1. 尝试将 delta 字符串解析为 Decimal
                            match raw_event.delta.parse::<Decimal>() {
                                Ok(delta) => {
                                    // 2. 如果解析成功，构建 Event 对象
                                    let event = Event::new(
                                        raw_event.slot,
                                        raw_event.tx_signature,
                                        raw_event.mint_address.to_string(),
                                        raw_event.account_address.to_string(),
                                        raw_event.owner_address.map_or("".to_string(), |o| o.to_string()),
                                        delta,
                                        raw_event.confirmed,
                                    );
                                    // 3. 将有效的元组包裹在 Some 中返回，以保留此数据
                                    Some((message_id, event))
                                }
                                Err(e) => {
                                    // 如果解析失败，打印日志并返回 None，这条记录将被安全地丢弃
                                    info!(
                                        "Skipping event with invalid delta. Tx: {}, Delta: '{}', Error: {}",
                                        raw_event.tx_signature, raw_event.delta, e
                                    );
                                    None
                                }
                            }
                        })
                        .unzip();


                    // --- 核心职责：将清洗后的数据存入 events 表 ---
                    if !token_events.is_empty() {
                        // 只更新 events 表
                        if let Err(e) = self.database.upsert_events_batch(&token_events, &mut logger).await {
                            error!("Error upserting events: {}", e);
                            // 如果写入数据库失败，我们不应该 ACK 消息，让它可以被重新处理
                            continue;
                        };
                            if let Err(e) = self
                        .database
                        .upsert_token_accounts_batch(&token_events)
                        .await
                    {
                        error!("upsert token accounts batch failed: {}", e);
                        continue;
                    };
                    let holder_counts = match self.database.upsert_holder_batch(&token_events).await {
                        Ok(h) => h,
                        Err(e) => {
                            error!("upsert holder batch failed:{}", e);
                            return Err(e);
                        }
                    };
                    let slot = token_events[0].slot;
                    if let Err(e) = self
                        .database
                        .update_mint_stats_holder_count(&holder_counts, slot as i64)
                        .await
                    {
                        error!("Error update mint stats holder count: {}", e);
                    };
                        logger.log("sql upsert events complete");

                        // 数据库写入成功之后才能 ACK
                        if let Err(e) = self.redis.ack_messages(&message_ids).await {
                            error!("Error acknowledging messages: {}", e);
                            // ACK 失败是一个严重问题，需要考虑如何处理（重试或告警）
                            continue;
                        };
                        logger.log("redis ack messages complete");
                        info!("✅ Monitor: Ingested {} events into the database.", token_events.len());
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn build_baseline(&self, mint: &str) -> anyhow::Result<i64> {
        let token_accounts = self.http_client.get_token_holders(mint).await?;

        let mut logger = TaskLogger::new("baseline", "3");

        if !token_accounts.is_empty() {
            if let Err(e) = self
                .database
                .establish_token_accounts_baseline(&token_accounts, &mut logger)
                .await
            {
                error!("Error establish token accounts baseline: {}", e);
            }

            let holder_account = match self
                .database
                .establish_holders_baseline(&token_accounts, &mut logger)
                .await
            {
                Ok(h) => h,
                Err(e) => {
                    error!("Error establish holders baseline: {}", e);
                    return Err(e);
                }
            };

            let last_updated_slot = token_accounts[0].slot;

            if let Err(e) = self
                .database
                .establish_mint_stats_baseline(mint, holder_account as i64, last_updated_slot)
                .await
            {
                error!("establish mint stats baseline: {}", e);
            }
        }

        let baseline_slot = token_accounts[0].slot;
        Ok(baseline_slot)
    }

    // baseline 获取之后，下一步是将 baseline slot -> 当前 monitor slot之间的数据进行整合
    pub async fn catch_up(&self, baseline_slot: i64, mint: &str) -> anyhow::Result<()> {
        const BATCH_SIZE: i64 = 1000; // 每次处理 1000 条事件
        // 初始化游标，(start_slot - 1, i64::MAX) 是一个安全的起点
        let mut cursor = (baseline_slot - 1, i64::MAX);

        loop {
            let mut logger = TaskLogger::new("sync controller-catch up", "2");
            let (token_events, next_cursor) = match self
                .database
                .get_next_events_batch(cursor, mint, BATCH_SIZE)
                .await
            {
                Ok((token_events, next_cursor)) => match next_cursor {
                    Some(next_cursor) => (token_events, next_cursor),
                    None => {
                        info!("next_cursor is Null, wait for a wheal");
                        tokio::time::sleep(Duration::from_secs(200)).await;
                        continue;
                    }
                },
                Err(e) => {
                    error!("get next events batch");
                    return Err(e);
                }
            };

            if let Err(e) = self
                .database
                .upsert_token_accounts_batch(&token_events)
                .await
            {
                error!("upsert token accounts batch failed: {}", e);
                continue;
            }
            let holder_counts = match self.database.upsert_holder_batch(&token_events).await {
                Ok(h) => h,
                Err(e) => {
                    error!("upsert holder batch failed:{}", e);
                    return Err(e);
                }
            };
            let slot = token_events[0].slot;
            if let Err(e) = self
                .database
                .update_mint_stats_holder_count(&holder_counts, slot as i64)
                .await
            {
                error!("Error update mint stats holder count: {}", e);
            }

            logger.log("sql upsert events complete");

            // 更新游标为函数返回的新游标
            cursor = next_cursor;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sync_controller() {}
}
