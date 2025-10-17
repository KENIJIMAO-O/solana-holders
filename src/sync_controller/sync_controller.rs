use std::collections::HashSet;
use crate::baseline::getProgramAccounts::HttpClient;
use crate::database::postgresql::DatabaseConnection;
use crate::message_queue::token_event_message_queue::Redis;
use crate::repositories::events::{Event, EventsRepository};
use crate::repositories::holders::HoldersRepository;
use crate::repositories::mint_stats::MintStatsRepository;
use crate::repositories::token_accounts::TokenAccountsRepository;
use crate::repositories::tracked_mints::TrackedMintsRepository;
use crate::utils::timer::TaskLogger;
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use crate::repositories::AtomicityData;

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
        let consumer_name = "token_event_dequeuer";

        loop {
            let mut logger = TaskLogger::new("sync_controller", "2");

            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Monitor received cancellation signal. Shutting down...");
                    break;
                }

                datas_result = self.redis.batch_dequeue_holder_event(
                    consumer_name,
                    BATCH_SIZE,
                    BATCH_TIMEOUT_MS as usize,
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

                    // 如果为空，说明数据还没进队
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
                                        raw_event.tx_signature.clone(),
                                        raw_event.mint_address.to_string(),
                                        raw_event.account_address.to_string(),
                                        raw_event.owner_address.map_or("".to_string(), |o| o.to_string()),
                                        delta,
                                        raw_event.confirmed,
                                    );
                                    // 3. 将有效的元组包裹在 Some 中返回，以保留此数据
                                    Some((message_id.clone(), event))
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

                    // 将新代币发送到 baseline 消息队列中
                    let mints: Vec<String> = token_events
                        .iter()
                        .map(|token_event| token_event.mint_pubkey.clone())
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect();
                    let untracked_mints = self.database.is_tracked_batch(&mints).await?;
                    self.redis.batch_enqueue_baseline_task(&untracked_mints).await?;

                    // --- 核心职责：将新的数据更新到数据库中 ---
                    // todo!: 这里我想说的就是，对于任意一个数据，一定会进events表，如果这个代币已经构建了baseline，那么可以直接利用从token queue获取的数据更新
                    // todo!: 如果没有构建baseline，那么就不用更新，等到构建完之后，catch-up需要从数据中将所有和他相关的events全部合并之后，回到token queue
                    // todo!: 所以对于数据库中的数据需要更新三次，第一是baseline构建的时候的更新，第二是catch-up时候的更新，最后是当前函数中token queue的更新
                    if !token_events.is_empty() {
                        // 对于events表，无论当前代币处于 Not started baseline_building catching_up synced 任意一个阶段，都需要更新
                        match self.database.upsert_events_batch(&token_events, &mut logger).await {
                            Ok(()) => {
                                // ack token_queue message
                                 if let Err(e) = self.redis.ack_messages(&message_ids).await {
                                    error!("Error acknowledging messages: {}", e);
                                    // ACK 失败是一个严重问题，需要考虑如何处理（重试或告警）
                                    continue;
                                };
                            } ,
                            Err(e) => {
                                error!("Error upserting events: {}", e);
                                // 如果写入数据库失败，我们不应该 ACK 消息，让它可以被重新处理
                                continue;
                            }
                        };
                        logger.log("sql upsert events complete");

                        // 对于其他的几个表，必须等到代币完成catch-up之后，即tracked_mints.status == synced 才能在这里更新
                        let synced_mints = self.database.filter_synced_mints(&mints).await?;
                        let synced_mints_set: HashSet<&str> = synced_mints.iter().map(|s| s.as_str()).collect();

                        let synced_token_events: Vec<Event> = token_events
                            .into_iter()
                            .filter(|token_event| {
                                synced_mints_set.contains(token_event.mint_pubkey.as_str())
                            })
                            .collect();

                        if synced_token_events.is_empty() { continue }
                        if let Err(e) = self.database.upsert_synced_mints_atomic(&synced_token_events).await {
                            error!("upsert token_account, holders, mint_stats error: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn consume_baseline_mints_for_queue(
        &self,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        const MAX_CONCURRENT_BASELINE: usize = 10; // 最大并发数
        const DEQUEUE_SIZE: usize = 50; // 批量拉取任务数
        const BATCH_TIMEOUT_MS: u64 = 100;
        let consumer_name = "baseline_dequeuer";

        // 创建信号量控制并发数
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_BASELINE));

        loop {
            let mut logger = TaskLogger::new("sync_controller", "3");

            tokio::select! {
                // 分支1 收到了取消信息
                _ = cancellation_token.cancelled() => {
                    info!("baseline consumer received cancellation signal. Shutting down...");
                    break;
                }

                mints_result = self.redis.batch_dequeue_baseline_task(
                    consumer_name,
                    DEQUEUE_SIZE,
                    BATCH_TIMEOUT_MS as usize,
                ) => {
                    let mints = match mints_result {
                        Ok(m) => m,
                        Err(e) => {
                            error!("Failed to dequeue in baseline consumer: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    if mints.is_empty() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    logger.log(&format!("Dequeued {} mints for baseline processing", mints.len()));

                    // 使用 Semaphore 控制并发，spawn 多个任务
                    let mut handles = Vec::new();

                    for (message_id, mint) in mints {
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(e) => {
                                error!("Failed to acquire semaphore: {}", e);
                                continue;
                            }
                        };
                        let controller = self.clone();

                        let handle = tokio::spawn(async move {
                            let _permit = permit; // 持有 permit，函数结束时自动释放

                            // 完整的 baseline 处理流程
                            let result = Self::process_single_baseline(controller, &mint).await;

                            (message_id, mint, result)
                        });

                        handles.push(handle);
                    }

                    // 等待所有任务完成
                    logger.log("Waiting for all baseline tasks to complete");
                    let results = futures::future::join_all(handles).await;

                    // 统计成功和失败
                    let mut success_count = 0;
                    let mut fail_count = 0;

                    for result in results {
                        match result {
                            Ok((message_id, mint, Ok(_))) => {
                                // 处理成功，ACK 消息
                                if let Err(e) = self.redis.ack_message_baseline(&message_id).await {
                                    error!("Failed to ACK message {} for mint {}: {}", message_id, mint, e);
                                } else {
                                    info!("✅ Baseline completed for mint: {}", mint);
                                    success_count += 1;
                                }
                            }
                            Ok((_message_id, mint, Err(e))) => {
                                // 处理失败，不 ACK，消息留在 PEL 等待重试
                                // todo!: 现在有一个问题就是对于USDT/USDC这种有点拉不下来
                                error!("❌ Baseline failed for mint {}: {}", mint, e);
                                fail_count += 1;
                            }
                            Err(e) => {
                                // Task panic
                                error!("⚠️ Baseline task panicked: {}", e);
                                fail_count += 1;
                            }
                        }
                    }

                    info!("Baseline batch complete: {} succeeded, {} failed", success_count, fail_count);
                }
            }
        }

        Ok(())
    }

    /// 处理单个 mint 的完整 baseline 流程
    async fn process_single_baseline(
        controller: SyncController,
        mint: &str,
    ) -> anyhow::Result<()> {
        // 步骤 1: 构建 baseline 数据
        let baseline_slot = match controller.build_baseline(mint).await {
            Ok(slot) => {
                info!("Baseline data fetched for mint {}, slot: {}", mint, slot);
                slot
            }
            Err(e) => {
                error!("Failed to build baseline for mint {}: {}", mint, e);
                return Err(e);
            }
        };

        // 步骤 2: 记录 baseline 开始状态
        if let Err(e) = controller
            .database
            .start_baseline_batch(&[mint.to_string()], &[baseline_slot])
            .await
        {
            error!("Failed to mark baseline start for mint {}: {}", mint, e);
            return Err(e);
        }

        // 步骤 3: 标记 baseline 完成，进入 catching_up 状态
        if let Err(e) = controller
            .database
            .finish_baseline_batch(&[mint.to_string()])
            .await
        {
            error!("Failed to mark baseline finish for mint {}: {}", mint, e);
            return Err(e);
        }

        // 步骤 4: 执行 catch-up，追赶历史事件
        if let Err(e) = controller.catch_up(baseline_slot, mint).await {
            error!("Failed to catch up for mint {}: {}", mint, e);
            return Err(e);
        }

        // 步骤 5: 标记 catch-up 完成，进入 synced 状态
        if let Err(e) = controller
            .database
            .finish_catch_up_batch(&[mint.to_string()])
            .await
        {
            error!("Failed to mark catch up finish for mint {}: {}", mint, e);
            return Err(e);
        }

        info!("✅ Full baseline pipeline completed for mint: {}", mint);
        Ok(())
    }

    pub async fn build_baseline(&self, mint: &str) -> anyhow::Result<i64> {
        let token_accounts = self.http_client.get_token_holders(mint).await?;

        let baseline_slot = if !token_accounts.is_empty() {
            // 使用原子性方法建立 baseline，确保三张表同时成功或同时失败
            self.database
                .establish_baseline_atomic(mint, &token_accounts)
                .await?
        } else {
            0
        };

        Ok(baseline_slot)
    }

    /// 从 baseline_slot 追赶到当前已有的历史事件
    /// 当 next_cursor 为 None 时表示历史数据已追完，直接退出
    /// 之后的新事件由 consume_events_from_queue 统一处理
    pub async fn catch_up(&self, baseline_slot: i64, mint: &str) -> anyhow::Result<()> {
        const BATCH_SIZE: i64 = 1000;
        let mut cursor = (baseline_slot - 1, i64::MAX);
        let mut total_processed = 0;

        info!("Starting catch-up for mint {} from slot {}", mint, baseline_slot);

        loop {
            match self.database.get_next_events_batch(cursor, mint, BATCH_SIZE).await {
                Ok((token_events, Some(next_cursor))) => {
                    // 有更多历史数据，继续处理
                    if token_events.is_empty() {
                        cursor = next_cursor;
                        continue;
                    }

                    self.database.upsert_synced_mints_atomic(&token_events).await?;
                    total_processed += token_events.len();
                    cursor = next_cursor;
                }
                Ok((token_events, None)) => {
                    // 没有更多历史数据，处理最后一批后退出
                    if !token_events.is_empty() {
                        self.database.upsert_synced_mints_atomic(&token_events).await?;
                        total_processed += token_events.len();
                    }

                    info!("Catch-up completed for mint {}: processed {} events", mint, total_processed);
                    break;
                }
                Err(e) => {
                    error!("Failed to get events batch for mint {}: {}", mint, e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{fmt, EnvFilter, Layer};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use crate::database::postgresql::DatabaseConfig;
    use crate::message_queue::RedisQueueConfig;
    use super::*;


    fn set_up(){
        dotenv::dotenv().ok();
        let console_subscriber = fmt::layer()
            .with_target(false)
            .with_level(false)
            .with_writer(std::io::stdout);
        tracing_subscriber::registry()
            .with(
                console_subscriber.with_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info,rustls=warn,sqlx=warn,hyper=warn,tokio=warn".into()),
                ),
            )
            .init();
    }

    #[tokio::test]
    async fn test_consume_baseline_mints_for_queue() {
        set_up();
        // 创建消息队列
        let redis_url = std::env::var("REDIS_URL");
        let config = RedisQueueConfig::default();
        let message_queue = Arc::new(Redis::new(&redis_url.unwrap(), config).await.unwrap());
        let _ = message_queue.init_baseline_queue().await.unwrap();

        let db_url = std::env::var("DATABASE_URL").unwrap();
        let database_config = DatabaseConfig::new_optimized(db_url);
        let database = Arc::new(DatabaseConnection::new(database_config).await.unwrap());

        let http_rpc = std::env::var("RPC_URL").unwrap();
        let http_client = Arc::new(HttpClient::new(http_rpc).unwrap());

        let sync_controller =
            SyncController::new(message_queue.clone(), database.clone(), http_client.clone());

        let cancellation_token = CancellationToken::new();
        let token = cancellation_token.child_token();
        if let Err(e) = sync_controller.consume_baseline_mints_for_queue(token).await {
            error!("Monitor error: {:?}", e);
        }
    }
}
