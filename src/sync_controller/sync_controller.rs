use rust_decimal::Decimal;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use crate::database::postgresql::DatabaseConnection;
use crate::database::repositories::AtomicityData;
use crate::database::repositories::tracked_mints::TrackedMintsRepository;
use crate::baseline::HttpClient;
use crate::{app_error, app_info, BIG_TOKEN_HOLDER_COUNT};
use crate::clickhouse::clickhouse::{ClickHouse, Event};
use crate::database::repositories::mint_stats::MintStatsRepository;
use crate::kafka::KafkaMessageQueue;
use crate::error::Result;

#[derive(Clone)]
pub struct SyncController {
    pub kafka_queue: Arc<KafkaMessageQueue>,
    pub database: Arc<DatabaseConnection>,
    pub clickhouse: Arc<ClickHouse>,
    pub http_client: Arc<HttpClient>,
}

impl SyncController {
    pub fn new(
        kafka_queue: Arc<KafkaMessageQueue>,
        database: Arc<DatabaseConnection>,
        clickhouse: Arc<ClickHouse>,
        http_client: Arc<HttpClient>,
    ) -> Self {
        Self {
            kafka_queue,
            database,
            clickhouse,
            http_client,
        }
    }

    pub async fn consume_events_from_queue(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // 从环境变量读取配置，提供默认值
        let batch_size = std::env::var("TOKEN_EVENT_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let batch_timeout_ms = std::env::var("TOKEN_EVENT_BATCH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        let max_consecutive_failures = std::env::var("TOKEN_EVENT_MAX_FAILURES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let consumer_name = "token_event_dequeuer";

        app_info!(
            "🔧 Token Event Consumer 配置: batch_size={}, timeout={}ms, max_failures={}",
            batch_size, batch_timeout_ms, max_consecutive_failures
        );

        'retry_loop: loop {
            let mut consecutive_failures = 0;

            loop{
                tokio::select! {
                _ = cancellation_token.cancelled() => {
                    app_info!("Monitor received cancellation signal. Shutting down...");
                    break 'retry_loop;
                }

                datas_result = self.kafka_queue.batch_dequeue_holder_event(
                    consumer_name,
                    batch_size,
                    batch_timeout_ms as usize,
                ) => {
                    let datas = match datas_result {
                        Ok(d) => {
                            consecutive_failures = 0;
                            d
                        },
                        Err(e) => {
                            consecutive_failures += 1;
                            if consecutive_failures >= max_consecutive_failures {
                                app_error!("Failed to dequeue {} times in a row. Restarting consumer.", max_consecutive_failures);
                                break;
                            }
                            app_error!("Failed to dequeue from Redis: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue; // 继续下一次循环
                        }
                    };

                    // 如果为空，说明数据还没进队
                    if datas.is_empty() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }

                    app_info!("batch dequeue holder events complete");

                    // --- 数据清洗、转换和聚合（单次迭代）---
                    let capacity = datas.len();
                    let mut _message_ids = Vec::with_capacity(capacity);
                    let mut token_events = Vec::with_capacity(capacity);
                    let mut unique_mints = HashSet::new();

                    for (message_id, raw_event) in datas {
                        match raw_event.delta.parse::<Decimal>() {
                            Ok(delta) => {
                                let confirmed_u8 = if raw_event.confirmed { 1u8 } else { 0u8 };
                                let event = Event::new(
                                    raw_event.slot,
                                    raw_event.tx_signature, // move
                                    raw_event.mint_address.to_string(),
                                    raw_event.account_address.to_string(),
                                    raw_event.owner_address.map_or("".to_string(), |o| o.to_string()),
                                    delta,
                                    confirmed_u8,
                                );
                                unique_mints.insert(event.mint_pubkey.clone()); // Clone for the set
                                token_events.push(event); // Move event
                                _message_ids.push(message_id); // Move message_id
                            }
                            Err(e) => {
                                // todo!: 这里其实也有问题，因为直接丢弃一个事件的话，其实很有可能导致相关代币后续所有信息更新全错
                                app_error!(
                                    "Skipping event with invalid delta. Tx: {}, Delta: '{}', Error: {}",
                                    raw_event.tx_signature, raw_event.delta, e
                                );
                            }
                        }
                    }

                    // 从 HashSet 创建最终的 mints Vec
                    let mints: Vec<String> = unique_mints.into_iter().collect();

                    let untracked_mints = self.database.is_tracked_batch(&mints).await?;
                    app_info!("{}", &format!("untracked_mints_len:{}", untracked_mints.len()));

                    self.kafka_queue.batch_enqueue_baseline_task(&untracked_mints).await?;
                    app_info!("complete batch enqueue baseline");

                    // --- 核心职责：将新的数据更新到数据库中 ---
                    // 这里我想说的就是，对于任意一个数据，一定会进events表，如果这个代币已经构建了baseline，那么可以直接利用从token queue获取的数据更新
                    // 如果没有构建baseline，那么就不用更新，等到构建完之后，catch-up需要从数据中将所有和他相关的events全部合并之后，回到token queue
                    // 所以对于数据库中的数据需要更新三次，第一是baseline构建的时候的更新，第二是catch-up时候的更新，最后是当前函数中token queue的更新
                    if !token_events.is_empty() {

                        // 对于events表，无论当前代币处于 Not_started baseline_building catching_up synced 任意一个阶段，都需要更新
                        match self.clickhouse.upsert_events_batch(&token_events).await {
                            Ok(()) => {
                                // ack token_queue message
                                 if let Err(e) = self.kafka_queue.ack_token_events(consumer_name).await {
                                    app_error!("Error acknowledging messages: {}", e);
                                    // ACK 失败是一个严重问题，需要考虑如何处理（重试或告警）
                                    continue;
                                };
                            } ,
                            Err(e) => {
                                app_error!("Error upserting events: {}", e);
                                // 如果写入数据库失败，我们不应该 ACK 消息，让它可以被重新处理
                                continue;
                            }
                        };
                        app_info!("sql upsert events complete");

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

                        // 这俩可能需要绑定在一块
                        if let Err(e) = self.database.upsert_synced_mints_atomic(&synced_token_events, &self.clickhouse).await {
                            app_error!("upsert token_account, holders, mint_stats error: {}", e);
                        }

                        app_info!("sql upsert token_account, holders, mint_stats complete");
                        }
                    }
                }
            }
            app_error!("Event consumer loop failed. Retrying in 10 seconds...");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }

    pub async fn consume_baseline_mints_for_queue(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // 从环境变量读取配置，提供默认值
        let max_concurrent = std::env::var("BASELINE_MAX_CONCURRENT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        let max_tasks_in_memory = std::env::var("BASELINE_MAX_TASKS_MEMORY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let dequeue_size = std::env::var("BASELINE_DEQUEUE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        let batch_timeout_ms = std::env::var("BASELINE_BATCH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        let max_consecutive_failures = std::env::var("BASELINE_MAX_FAILURES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let consumer_name = "baseline_dequeuer";

        app_info!(
            "🔧 Baseline Consumer 配置: max_concurrent={}, max_tasks_memory={}, dequeue_size={}, timeout={}ms, max_failures={}",
            max_concurrent, max_tasks_in_memory, dequeue_size, batch_timeout_ms, max_consecutive_failures
        );

        // execution_semaphore: 控制同时执行的任务数
        let execution_semaphore = Arc::new(Semaphore::new(max_concurrent));
        // memory_semaphore: 控制内存中的任务总数
        // 如果不要内存控制会导致loop一直出队，调度tokio::spawn，虽然这些任务会因为execution_semaphore的存在不会同时执行，但是一样会导致内存无限制的增加
        let memory_semaphore = Arc::new(Semaphore::new(max_tasks_in_memory));

        tokio::time::sleep(Duration::from_secs(10)).await; // 等待消息队列中有一些值

        'retry_loop: loop {
            let mut consecutive_failures = 0;

            loop {
                tokio::select! {
                    // 分支1 收到了取消信息
                    _ = cancellation_token.cancelled() => {
                        app_info!("baseline consumer received cancellation signal. Shutting down...");
                        break 'retry_loop;
                    }

                    mints_result = self.kafka_queue.batch_dequeue_baseline_task(
                        consumer_name,
                        dequeue_size,
                        batch_timeout_ms as usize,
                    ) => {
                        let mints = match mints_result {
                            Ok(m) => {
                                consecutive_failures = 0;
                                m
                            },
                            Err(e) => {
                                app_error!("Failed to dequeue in baseline consumer: {}", e);
                                consecutive_failures += 1;
                                if consecutive_failures >= max_consecutive_failures {
                                    app_error!("Failed to dequeue {} times in a row. Restarting consumer.", max_consecutive_failures);
                                    break;
                                }
                                continue;
                            }
                        };

                        if mints.is_empty() {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }

                        app_info!("Dequeued {} mints for baseline processing, memory available: {}/{}",
                            mints.len(),
                            memory_semaphore.available_permits(),
                            max_tasks_in_memory
                        );

                        for (_message_id, mint) in mints {
                            let controller = self.clone();
                            let exec_sem = execution_semaphore.clone();
                            let mem_sem = memory_semaphore.clone();

                            // 先获取内存permit，如果内存中任务数达到100，主循环会在这里阻塞
                            // 当某个任务完成时，会释放memory_permit，主循环恢复
                            let memory_permit = match mem_sem.acquire_owned().await {
                                Ok(p) => p,
                                Err(e) => {
                                    app_error!("Failed to acquire memory semaphore: {}", e);
                                    continue;
                                }
                            };

                            let self_clone = self.clone();
                            tokio::spawn(async move {
                                // 持有memory_permit，任务结束时自动释放
                                let _mem_permit = memory_permit;

                                // 在任务内部获取执行permit，不阻塞主循环
                                let exec_permit = match exec_sem.acquire_owned().await {
                                    Ok(p) => p,
                                    Err(e) => {
                                        app_error!("Failed to acquire execution semaphore for mint {}: {}", mint, e);
                                        return;
                                    }
                                };
                                let _exec_permit = exec_permit;

                                // 核心处理
                                let result = self_clone.process_single_baseline(&mint, false).await;

                                // 处理完成后立即ACK
                                match result {
                                    Ok(_) => {
                                        // todo!: 暂时对没有处理的大代币也进行ack
                                        if let Err(e) = controller.kafka_queue.ack_baseline_tasks(consumer_name).await {
                                            app_error!("Failed to ACK message {} for mint {}: {}", consumer_name, mint, e);
                                        } else {
                                            app_info!("✅ Baseline completed for mint: {}", mint);
                                        }
                                    }
                                    Err(e) => {
                                        app_error!("❌ Baseline failed for mint {}: {}", mint, e);
                                    }
                                }
                                // _mem_permit 在这里drop，释放内存槽位
                            });
                        }
                    }
                }
            }
            app_error!("Baseline consumer loop failed. Retrying in 10 seconds...");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }

    /// 处理单个 mint 的完整 baseline 流程
    pub async fn process_single_baseline(&self, mint: &str, is_find: bool) -> Result<i64> {
        // 如果数量大于某个阈值，则视为big token，使用其他方式获取
        let onchain_holder_count = self.http_client.get_sol_scan_holder(mint).await?;
        if onchain_holder_count >= *BIG_TOKEN_HOLDER_COUNT {
            // 如果是大代币，直接返回solscan的值
            app_info!("big token:{}, holder count: {}", mint, onchain_holder_count);
            return Ok(onchain_holder_count as i64);
        }

        // 步骤 1: 构建 baseline 数据
        let baseline_slot = match self.build_baseline(mint).await {
            Ok(slot) => {
                app_info!("Baseline data fetched for mint {}, slot: {}", mint, slot);
                slot
            }
            Err(e) => {
                app_error!("Failed to build baseline for mint {}: {}", mint, e);
                return Err(e);
            }
        };

        // 步骤 2: 记录 baseline 开始状态
        if let Err(e) = self
            .database
            .start_baseline_batch(&[mint.to_string()], &[baseline_slot])
            .await
        {
            app_error!("Failed to mark baseline start for mint {}: {}", mint, e);
            return Err(e);
        }

        // 步骤 3: 标记 baseline 完成，进入 catching_up 状态
        if let Err(e) = self
            .database
            .finish_baseline_batch(&[mint.to_string()])
            .await
        {
            app_error!("Failed to mark baseline finish for mint {}: {}", mint, e);
            return Err(e);
        }

        // 步骤 4: 执行 catch-up，追赶历史事件
        if let Err(e) = self.catch_up(baseline_slot, mint).await {
            app_error!("Failed to catch up for mint {}: {}", mint, e);
            return Err(e);
        }

        // 步骤 5: 标记 catch-up 完成，进入 synced 状态
        if let Err(e) = self
            .database
            .finish_catch_up_batch(&[mint.to_string()])
            .await
        {
            app_error!("Failed to mark catch up finish for mint {}: {}", mint, e);
            return Err(e);
        }

        app_info!("✅ Full baseline pipeline completed for mint: {}", mint);

        let mut return_count = onchain_holder_count as i64;
        if is_find {
            return_count = self.database.get_holder_account(mint).await?;
        }
        Ok(return_count)
    }

    pub async fn build_baseline(&self, mint: &str) -> Result<i64> {
        app_info!("start building baseline for: {}", mint);
        let token_accounts = self.http_client.get_token_holders(mint).await?;

        let baseline_slot = if !token_accounts.is_empty() {
            // 使用原子性方法建立 baseline，确保三张表同时成功或同时失败
            self.database
                .establish_baseline_atomic(mint, token_accounts)
                .await?
        } else {
            0
        };

        Ok(baseline_slot)
    }

    /// 从 baseline_slot 追赶到当前已有的历史事件
    /// 当 next_cursor 为 None 时表示历史数据已追完，直接退出
    /// 之后的新事件由 consume_events_from_queue 统一处理
    pub async fn catch_up(&self, baseline_slot: i64, mint: &str) -> Result<()> {
        const BATCH_SIZE: i64 = 1000;
        let mut cursor = (baseline_slot - 1, i64::MAX);
        let mut total_processed = 0;

        app_info!(
            "Starting catch-up for mint {} from slot {}",
            mint, baseline_slot
        );

        // 在开始处理之前，将 baseline_slot 之前的所有未确认事件标记为 confirmed
        // 因为 baseline 已经代表了那个时刻的完整状态，这些过时的事件不需要再处理
        let skipped_count = self
            .clickhouse
            .confirm_events_before_baseline(mint, baseline_slot)
            .await?;
        if skipped_count > 0 {
            app_info!(
                "Skipped {} events before baseline_slot {} for mint {}",
                skipped_count, baseline_slot, mint
            );
        }

        loop {
            match self
                .clickhouse
                .get_next_events_batch(cursor, mint, BATCH_SIZE)
                .await
            {
                Ok((token_events, Some(next_cursor))) => {
                    // 有更多历史数据，继续处理
                    if token_events.is_empty() {
                        cursor = next_cursor;
                        continue;
                    }

                    app_info!("In next_cursor to sync mint atomic");
                    self.database
                        .upsert_synced_mints_atomic(&token_events, &self.clickhouse)
                        .await?;

                    total_processed += token_events.len();
                    cursor = next_cursor;
                }
                Ok((token_events, None)) => {
                    // 没有更多历史数据，处理最后一批后退出
                    app_info!("In none to sync mint atomic");
                    if !token_events.is_empty() {
                        self.database
                            .upsert_synced_mints_atomic(&token_events, &self.clickhouse)
                            .await?;
                        total_processed += token_events.len();
                    }

                    app_info!(
                        "Catch-up completed for mint {}: processed {} events",
                        mint, total_processed
                    );
                    break;
                }
                Err(e) => {
                    app_error!("Failed to get events batch for mint {}: {}", mint, e);
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

    // #[tokio::test]
    // async fn test_consume_baseline_mints_for_queue() {
    //     set_up();
    //     // 创建消息队列
    //     let redis_url = std::env::var("REDIS_URL");
    //     let config = RedisQueueConfig::default();
    //     let message_queue = Arc::new(Redis::new(&redis_url.unwrap(), config).await.unwrap());
    //     let _ = message_queue.init_baseline_queue().await.unwrap();
    //
    //     let db_url = std::env::var("DATABASE_URL").unwrap();
    //     let database_config = DatabaseConfig::new_optimized(db_url);
    //     let database = Arc::new(DatabaseConnection::new(database_config).await.unwrap());
    //
    //     let http_rpc = std::env::var("RPC_URL").unwrap();
    //     let http_client = Arc::new(HttpClient::default());
    //
    //     let sync_controller =
    //         SyncController::new(message_queue.clone(), database.clone(), http_client.clone());
    //
    //     let cancellation_token = CancellationToken::new();
    //     let token = cancellation_token.child_token();
    //     if let Err(e) = sync_controller
    //         .consume_baseline_mints_for_queue(token)
    //         .await
    //     {
    //         app_error!("Monitor error: {:?}", e);
    //     }
    // }
}