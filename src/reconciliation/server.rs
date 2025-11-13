use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use crate::baseline::HttpClient;
use crate::clickhouse::clickhouse::ClickHouse;
use crate::database::postgresql::DatabaseConnection;
use crate::database::repositories::AtomicityData;
use crate::reconciliation::model::{AppConfig, ReconciliationServer};
use crate::database::repositories::mint_stats::MintStatsRepository;
use crate::database::repositories::reconciliation_schedule::ReconciliationScheduleRepository;
use crate::error::{ReconciliationError, Result};


/// 手动对账结果
#[derive(Debug, Clone)]
pub struct ManualReconciliationResult {
    pub mint_pubkey: String,
    pub db_holder_count: i64,
    pub onchain_holder_count: Option<i64>, // RPC 失败时为 None
    pub difference: Option<i64>,
    pub difference_percentage: Option<f64>,
    pub success: bool,
    pub error_message: Option<String>,
}

impl ReconciliationServer {
    pub fn new(
        database: Arc<DatabaseConnection>,
        clickhouse: Arc<ClickHouse>,
        http_client: Arc<HttpClient>
    ) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name("config/default"))
            // 也可以从环境变量覆盖
            .add_source(config::Environment::with_prefix("APP"))
            .build()
            .map_err(|e| ReconciliationError::ServerCreationFailed(
                format!("Failed to read config: {}", e)
            ))?;

        let app_config = settings.try_deserialize::<AppConfig>()
            .map_err(|e| ReconciliationError::ServerCreationFailed(
                format!("Failed to read config: {}", e)
            ))?;

        Ok(Self{
            app_config,
            database,
            clickhouse,
            http_client,
        })
    }

    // 1.从表中获获取需要对账的mint，对其实行对账，对账完需要更新 对账时间
    pub async fn start_with_cancellation(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        const MAX_CONCURRENT_RECONCILIATION: usize = 10; // 最大并发数
        const MAX_DIFFERENCE: u64 = 10;                  // 对账允许误差的百分比（对于持有者越多的代币允许的误差就应该越小）
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_RECONCILIATION));

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("reconciliation server received cancellation signal. Shutting down...");
                    break;
                }
                data = self.database.get_due_mints() => {
                    match data {
                        Ok(due_mints) => {
                            if due_mints.is_empty() {
                                // 没有需要对账的 mint，等待一段时间后继续
                                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                                continue;
                            }

                            info!("Found {} mints due for reconciliation", due_mints.len());

                            // 提取所有 mint_pubkeys
                            let mint_pubkeys: Vec<String> = due_mints
                                .iter()
                                .map(|schedule| schedule.mint_pubkey.clone())
                                .collect();

                            // 批量查询当前数据库中的 holder_count
                            let db_holder_counts = match self.database.get_holder_counts_batch(&mint_pubkeys).await {
                                Ok(counts) => counts,
                                Err(e) => {
                                    error!("Failed to get holder counts from database: {}", e);
                                    continue;
                                }
                            };

                            // 构建 HashMap 方便查找
                            let db_counts_map: HashMap<String, i64> = db_holder_counts.into_iter().collect();

                            // 使用 Semaphore 控制并发，spawn 多个任务获取链上数据
                            let mut handles = Vec::new();

                            for schedule in due_mints {
                                let permit = match semaphore.clone().acquire_owned().await {
                                    Ok(p) => p,
                                    Err(e) => {
                                        error!("Failed to acquire semaphore: {}", e);
                                        continue;
                                    }
                                };
                                let http_client = self.http_client.clone();
                                let mint_pubkey = schedule.mint_pubkey.clone();

                                // 获取数据库中的当前 holder_count
                                let current_db_count = db_counts_map.get(&mint_pubkey).copied().unwrap_or(0);

                                // 进行对账
                                let handle = tokio::spawn(async move {
                                    let _permit = permit; // 持有 permit，函数结束时释放

                                    // todo!: 算了，暂时先直接通过api获取吧，后续在进行其他的考虑
                                    // let onchain_result = http_client.get_token_holders(&mint_pubkey).await;
                                    let onchain_result = http_client.get_sol_scan_holder(&mint_pubkey).await;
                                    (schedule, current_db_count, onchain_result)
                                });

                                handles.push(handle);
                            }

                            // 等待所有任务完成
                            info!("Waiting for all reconciliation tasks to complete");
                            let results = futures::future::join_all(handles).await;

                            // 处理结果
                            for result in results {
                                match result {
                                    Ok((schedule, current_db_count, onchain_result)) => {
                                        match onchain_result {
                                            Ok(holders_count) => {
                                                let holders_count = holders_count as i64;
                                                // let onchain_holder_count = holders.len() as i64;

                                                // 计算变化百分比（数据库变化）
                                                let change_percentage = if schedule.last_holder_count > 0 {
                                                    (current_db_count - schedule.last_holder_count).abs() as f64
                                                        / schedule.last_holder_count as f64 * 100.0
                                                } else {
                                                    0.0
                                                };

                                                // todo!: 这里要判断数据库中的人数与链上人数是否正确，目前这样的判断方式有点草率，因为没有考虑slot的问题
                                                // 如果系统更新的slot本身就要比 get_holders_count 要慢，那人数很可能就对不上
                                                // 如果是这样情况，先搞个数据库表将get_holders_count的数据存起来？然后等到执行到了这个slot再拿出来比较？
                                                let difference = if current_db_count > holders_count {
                                                    current_db_count - holders_count
                                                } else { holders_count - current_db_count};

                                                // 检查是否需要重建（避免除零错误）
                                                let needs_rebuild = if current_db_count == 0 {
                                                    // 数据库为空但链上有 holder，需要重建
                                                    if holders_count > 0 {
                                                        warn!(
                                                            "Token:{} has {} holders onchain but 0 in db, needs rebuild",
                                                            &schedule.mint_pubkey,
                                                            holders_count
                                                        );
                                                        true
                                                    } else {
                                                        // 两者都为 0，不需要重建
                                                        false
                                                    }
                                                } else {
                                                    // 正常计算差异百分比
                                                    let difference_percentage = difference * 100 / current_db_count;
                                                    if difference_percentage > MAX_DIFFERENCE as i64 {
                                                        error!(
                                                            "Token:{} count in db {} is not same as onchain count {}, difference: {}%",
                                                            &schedule.mint_pubkey,
                                                            current_db_count,
                                                            holders_count,
                                                            difference_percentage
                                                        );
                                                        true
                                                    } else {
                                                        false
                                                    }
                                                };

                                                // 如果需要重建，执行重建流程
                                                if needs_rebuild {
                                                    info!(
                                                        "Starting rebuild for mint {} due to significant difference",
                                                        &schedule.mint_pubkey
                                                    );

                                                    // 重新获取完整的 holder 列表用于重建
                                                    match self.http_client.get_token_holders(&schedule.mint_pubkey).await {
                                                        Ok(holders) => {
                                                            if !holders.is_empty() {
                                                                match self.database
                                                                    .establish_baseline_atomic(&schedule.mint_pubkey, &holders)
                                                                    .await
                                                                {
                                                                    Ok(baseline_slot) => {
                                                                        info!(
                                                                            "Rebuilt baseline for mint {} at slot {}",
                                                                            &schedule.mint_pubkey, baseline_slot
                                                                        );

                                                                        // 执行 catch-up
                                                                        if let Err(e) = self.catch_up(baseline_slot, &schedule.mint_pubkey).await {
                                                                            error!(
                                                                                "Failed to catch up for mint {} after rebuild: {}",
                                                                                &schedule.mint_pubkey, e
                                                                            );
                                                                        } else {
                                                                            info!(
                                                                                "✅ Successfully rebuilt and caught up for mint {}",
                                                                                &schedule.mint_pubkey
                                                                            );
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        error!(
                                                                            "Failed to establish baseline for mint {}: {}",
                                                                            &schedule.mint_pubkey, e
                                                                        );
                                                                    }
                                                                }
                                                            } else {
                                                                warn!(
                                                                    "Got empty holders list for mint {}, skipping rebuild",
                                                                    &schedule.mint_pubkey
                                                                );
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                "Failed to get token holders for rebuild of mint {}: {}",
                                                                &schedule.mint_pubkey, e
                                                            );
                                                        }
                                                    }
                                                }

                                                // 根据一段时间内的代币持有者人数变化百分比决定下次对账间隔
                                                let next_interval_hours = Self::determine_next_interval(change_percentage, &self.app_config);

                                                // 更新 schedule
                                                if let Err(e) = self.database.update_schedule_after_reconciliation(
                                                    &schedule.mint_pubkey,
                                                    current_db_count as i64,
                                                    next_interval_hours
                                                ).await {
                                                    error!("Failed to update schedule for mint {}: {}", schedule.mint_pubkey, e);
                                                } else {
                                                    info!(
                                                        "✅ Reconciliation completed for mint {}: onchain={}, db={}, next_interval={}h",
                                                        schedule.mint_pubkey, holders_count, current_db_count, next_interval_hours
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                // RPC 调用失败，仍然更新 schedule（基于数据库变化）
                                                warn!("Failed to get onchain data for mint {}: {}", schedule.mint_pubkey, e);

                                                let change_percentage = if schedule.last_holder_count > 0 {
                                                    (current_db_count - schedule.last_holder_count).abs() as f64
                                                        / schedule.last_holder_count as f64 * 100.0
                                                } else {
                                                    0.0
                                                };

                                                let next_interval_hours = Self::determine_next_interval(change_percentage, &self.app_config);

                                                if let Err(e) = self.database.update_schedule_after_reconciliation(
                                                    &schedule.mint_pubkey,
                                                    current_db_count,
                                                    next_interval_hours
                                                ).await {
                                                    error!("Failed to update schedule for mint {}: {}", schedule.mint_pubkey, e);
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Task panicked: {}", e);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!("Failed to get due mints: {:?}", err);
                            // 不直接返回错误，而是等待后重试
                            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn determine_next_interval(change_percentage: f64, config: &AppConfig) -> i32 {
        for tier in &config.scheduling_tiers {
            if change_percentage >= tier.threshold_percent {
                return tier.interval_hours;
            }
        }
        config.default_interval_hours
    }

    /// 从 baseline_slot 追赶到当前已有的历史事件
    /// 当 next_cursor 为 None 时表示历史数据已追完，直接退出
    /// 之后的新事件由 consume_events_from_queue 统一处理
    pub async fn catch_up(&self, baseline_slot: i64, mint: &str) -> Result<()> {
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
            .clickhouse
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

                    info!("In next_cursor to sync mint atomic");
                    self.database
                        .upsert_synced_mints_atomic(&token_events, &self.clickhouse)
                        .await?;

                    total_processed += token_events.len();
                    cursor = next_cursor;
                }
                Ok((token_events, None)) => {
                    // 没有更多历史数据，处理最后一批后退出
                    info!("In none to sync mint atomic");
                    if !token_events.is_empty() {
                        self.database
                            .upsert_synced_mints_atomic(&token_events, &self.clickhouse)
                            .await?;
                        total_processed += token_events.len();
                    }

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

    /// 手动对账：对指定的 mints 立即执行对账检查
    ///
    /// 不更新 schedule 表，仅返回对账结果
    pub async fn reconcile_manual(
        &self,
        mint_pubkeys: &[String],
    ) -> Result<Vec<ManualReconciliationResult>> {
        if mint_pubkeys.is_empty() {
            return Ok(vec![]);
        }

        const MAX_CONCURRENT: usize = 10;
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));

        info!(
            "Starting manual reconciliation for {} mints",
            mint_pubkeys.len()
        );

        // 1. 批量查询数据库中的 holder_count
        let db_holder_counts = self.database.get_holder_counts_batch(mint_pubkeys)
            .await
            .map_err(|e| ReconciliationError::ReconcileManualFailed{
                method: "get holder counts batch error".to_string(),
                source: Box::from(e)
            })?;
        let db_counts_map: HashMap<String, i64> = db_holder_counts.into_iter().collect();

        // 2. 并发获取链上数据
        let mut handles = Vec::new();

        for mint_pubkey in mint_pubkeys {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(e) => {
                    error!("Failed to acquire semaphore: {}", e);
                    continue;
                }
            };
            let http_client = self.http_client.clone();
            let mint = mint_pubkey.clone();
            let db_count = db_counts_map.get(mint_pubkey).copied().unwrap_or(0);

            let handle = tokio::spawn(async move {
                let _permit = permit;

                let onchain_result = http_client.get_token_holders(&mint).await;

                (mint, db_count, onchain_result)
            });

            handles.push(handle);
        }

        // 3. 等待所有任务完成并处理结果
        let results = futures::future::join_all(handles).await;
        let mut reconciliation_results = Vec::new();

        for result in results {
            match result {
                Ok((mint_pubkey, db_count, onchain_result)) => match onchain_result {
                    Ok(holders) => {
                        let onchain_count = holders.len() as i64;
                        let difference = onchain_count - db_count;
                        let difference_percentage = if db_count > 0 {
                            Some(difference.abs() as f64 / db_count as f64 * 100.0)
                        } else {
                            None
                        };

                        reconciliation_results.push(ManualReconciliationResult {
                            mint_pubkey: mint_pubkey.clone(),
                            db_holder_count: db_count,
                            onchain_holder_count: Some(onchain_count),
                            difference: Some(difference),
                            difference_percentage,
                            success: true,
                            error_message: None,
                        });

                        info!(
                            "✅ Manual reconciliation for {}: onchain={}, db={}, diff={}",
                            mint_pubkey, onchain_count, db_count, difference
                        );
                    }
                    Err(e) => {
                        warn!("Failed to get onchain data for mint {}: {}", mint_pubkey, e);

                        reconciliation_results.push(ManualReconciliationResult {
                            mint_pubkey: mint_pubkey.clone(),
                            db_holder_count: db_count,
                            onchain_holder_count: None,
                            difference: None,
                            difference_percentage: None,
                            success: false,
                            error_message: Some(e.to_string()),
                        });
                    }
                },
                Err(e) => {
                    error!("Task panicked during manual reconciliation: {}", e);
                }
            }
        }

        info!(
            "Manual reconciliation completed: {}/{} successful",
            reconciliation_results.iter().filter(|r| r.success).count(),
            reconciliation_results.len()
        );

        Ok(reconciliation_results)
    }
}
