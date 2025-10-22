use crate::baseline::getProgramAccounts::HttpClient;
use crate::database::postgresql::DatabaseConnection;
use crate::message_queue::token_event_message_queue::Redis;
use crate::repositories::AtomicityData;
use crate::repositories::events::{Event, EventsRepository};
use crate::repositories::tracked_mints::TrackedMintsRepository;
use crate::utils::timer::TaskLogger;
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Clone)]
pub struct SyncController {
    pub redis: Arc<Redis>,
    pub database: Arc<DatabaseConnection>,
    pub http_client: Arc<HttpClient>,
}

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
            let mut logger = TaskLogger::new("sync_controller_events", "2");

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
                        // 确认当前events已经使用
                        self.database.confirm_events(&synced_token_events).await?;

                        logger.log("sql upsert token_account, holders, mint_stats error complete");
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
        // todo!: 这里考虑是否需要全局并发数，将这些配置在.env文件中
        // todo!: 这里考虑 分离大代币和 小代币，防止大代币长时间占用线程，可以通过创建两个不同的semaphore来实现
        const MAX_CONCURRENT_BASELINE: usize = 10; // 最大并发数
        const DEQUEUE_SIZE: usize = 50; // 批量拉取任务数
        const BATCH_TIMEOUT_MS: u64 = 100;
        let consumer_name = "baseline_dequeuer";

        // 创建信号量控制并发数
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_BASELINE));
        tokio::time::sleep(Duration::from_secs(10)).await;

        loop {
            let mut logger = TaskLogger::new("sync_controller_baseline", "3");

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
                    info!("Dequeued {} mints for baseline processing", mints.len());

                    for (message_id, mint) in mints {
                        let controller = self.clone();
                        let semaphore_clone = semaphore.clone();

                        tokio::spawn(async move {
                            // 在任务内部等待semaphore permit，不阻塞主循环
                            let permit = match semaphore_clone.acquire_owned().await {
                                Ok(p) => p,
                                Err(e) => {
                                    error!("Failed to acquire semaphore for mint {}: {}", mint, e);
                                    return;  // 直接返回，不继续处理
                                }
                            };
                            let _permit = permit;

                            let result = Self::process_single_baseline(controller.clone(), &mint).await;

                            // 处理完成后立即ACK，不等待其他代币
                            match result {
                                Ok(_) => {
                                    if let Err(e) = controller.redis.ack_message_baseline(&message_id).await {
                                        error!("Failed to ACK message {} for mint {}: {}", message_id, mint, e);
                                    } else {
                                        info!("✅ Baseline completed for mint: {}", mint);
                                    }
                                }
                                Err(e) => {
                                    // 处理失败，不 ACK，消息留在 PEL 等待重试
                                    error!("❌ Baseline failed for mint {}: {}", mint, e);
                                }
                            }
                        });
                    }
                    // 不等待任务完成，立即进入下一次循环拉取新代币
                }
            }
        }

        Ok(())
    }

    /// 处理单个 mint 的完整 baseline 流程
    async fn process_single_baseline(controller: SyncController, mint: &str) -> anyhow::Result<()> {
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
        info!("start building baseline for: {}", mint);
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

        info!(
            "Starting catch-up for mint {} from slot {}",
            mint, baseline_slot
        );

        // 在开始处理之前，将 baseline_slot 之前的所有未确认事件标记为 confirmed
        // 因为 baseline 已经代表了那个时刻的完整状态，这些过时的事件不需要再处理
        let skipped_count = self
            .database
            .confirm_events_before_baseline(mint, baseline_slot)
            .await?;
        if skipped_count > 0 {
            info!(
                "Skipped {} events before baseline_slot {} for mint {}",
                skipped_count, baseline_slot, mint
            );
        }

        loop {
            match self
                .database
                .get_next_events_batch(cursor, mint, BATCH_SIZE)
                .await
            {
                Ok((token_events, Some(next_cursor))) => {
                    // 有更多历史数据，继续处理
                    if token_events.is_empty() {
                        cursor = next_cursor;
                        continue;
                    }

                    info!("In next_cursor to sync mint atomic");
                    self.database
                        .upsert_synced_mints_atomic(&token_events)
                        .await?;

                    // 确认当前events已经使用
                    self.database.confirm_events(&token_events).await?;

                    total_processed += token_events.len();
                    cursor = next_cursor;
                }
                Ok((token_events, None)) => {
                    // 没有更多历史数据，处理最后一批后退出
                    info!("In none to sync mint atomic");
                    if !token_events.is_empty() {
                        self.database
                            .upsert_synced_mints_atomic(&token_events)
                            .await?;
                        total_processed += token_events.len();
                    }
                    // 确认当前events已经使用
                    self.database.confirm_events(&token_events).await?;

                    info!(
                        "Catch-up completed for mint {}: processed {} events",
                        mint, total_processed
                    );
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
    use super::*;
    use crate::database::postgresql::DatabaseConfig;
    use crate::message_queue::RedisQueueConfig;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, Layer, fmt};

    fn set_up() {
        dotenv::dotenv().ok();
        let console_subscriber = fmt::layer()
            .with_target(false)
            .with_level(false)
            .with_writer(std::io::stdout);
        tracing_subscriber::registry()
            .with(
                console_subscriber.with_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                        "info,rustls=warn,sqlx=warn,hyper=warn,tokio=warn".into()
                    }),
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
        if let Err(e) = sync_controller
            .consume_baseline_mints_for_queue(token)
            .await
        {
            error!("Monitor error: {:?}", e);
        }
    }
}
