use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use crate::{app_error, app_info, app_warn};
use crate::baseline::HttpClient;
use crate::clickhouse::clickhouse::ClickHouse;
use crate::database::postgresql::DatabaseConnection;
use crate::database::repositories::AtomicityData;
use crate::reconciliation::model::{AppConfig, ReconciliationServer};
use crate::database::repositories::mint_stats::MintStatsRepository;
use crate::database::repositories::reconciliation_schedule::ReconciliationScheduleRepository;
use crate::error::{ReconciliationError, Result};


/// 对账运行时配置
#[derive(Debug, Clone)]
struct ReconciliationConfig {
    max_concurrent: usize,
    max_difference_percent: u64,
    timeout_seconds: u64,
}

impl ReconciliationConfig {
    fn from_env() -> Self {
        let max_concurrent = std::env::var("RECONCILIATION_MAX_CONCURRENT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let max_difference_percent = std::env::var("RECONCILIATION_MAX_DIFFERENCE")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);

        let timeout_seconds = std::env::var("RECONCILIATION_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(300);

        Self {
            max_concurrent,
            max_difference_percent,
            timeout_seconds,
        }
    }
}

/// 单个对账任务的结果
#[derive(Debug)]
struct ReconciliationTaskResult {
    mint_pubkey: String,
    db_holder_count: i64,
    last_holder_count: i64,
    onchain_result: Result<u64>,
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

    pub async fn start_with_cancellation(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // 加载配置
        let config = ReconciliationConfig::from_env();

        app_info!(
            "🔧 Reconciliation 配置: max_concurrent={}, max_difference={}%, timeout={}s",
            config.max_concurrent, config.max_difference_percent, config.timeout_seconds
        );

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    app_info!("reconciliation server received cancellation signal. Shutting down...");
                    break;
                }
                data = self.database.get_due_mints(self.app_config.dues_batch_size) => {
                    match data {
                        Ok(due_mints) if !due_mints.is_empty() => {
                            // 处理这批对账任务
                            if let Err(e) = self.process_reconciliation_batch(due_mints, &config).await {
                                app_error!("Failed to process reconciliation batch: {}", e);
                            }
                        }
                        Ok(_) => {
                            // 没有需要对账的 mint，等待一段时间后继续
                            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                        }
                        Err(err) => {
                            app_error!("Failed to get due mints: {:?}", err);
                            // 不直接返回错误，而是等待后重试
                            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn calculate_change_percentage(current: i64, last: i64) -> f64 {
        if last > 0 {
            (current - last).abs() as f64 / last as f64 * 100.0
        } else {
            0.0
        }
    }

    fn determine_next_interval(change_percentage: f64, config: &AppConfig) -> i32 {
        for tier in &config.scheduling_tiers {
            if change_percentage >= tier.threshold_percent {
                return tier.interval_hours;
            }
        }
        config.default_interval_hours
    }

    /// 根据数据库持有者数量变化更新对账计划
    async fn update_schedule_based_on_db_change(
        &self,
        mint_pubkey: &str,
        current_db_count: i64,
        last_holder_count: i64,
    ) -> Result<i32> {
        let change_percentage = Self::calculate_change_percentage(
            current_db_count,
            last_holder_count
        );

        let next_interval_hours = Self::determine_next_interval(
            change_percentage,
            &self.app_config
        );

        self.database
            .update_schedule_after_reconciliation(
                mint_pubkey,
                current_db_count,
                next_interval_hours
            )
            .await?;

        Ok(next_interval_hours)
    }

    /// 批量查询数据库中的 holder counts
    async fn fetch_db_holder_counts(
        &self,
        mint_pubkeys: &[String],
    ) -> Result<HashMap<String, i64>> {
        let db_holder_counts = self.database.get_holder_counts_batch(mint_pubkeys).await?;
        Ok(db_holder_counts.into_iter().collect())
    }

    /// 并发获取链上数据
    async fn fetch_onchain_data_concurrently(
        &self,
        due_mints: Vec<crate::database::repositories::reconciliation_schedule::ReconciliationSchedule>,
        db_counts_map: &HashMap<String, i64>,
        config: &ReconciliationConfig,
    ) -> Vec<std::result::Result<ReconciliationTaskResult, tokio::task::JoinError>> {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
        let mut handles = Vec::new();

        for schedule in due_mints {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(e) => {
                    app_error!("Failed to acquire semaphore: {}", e);
                    continue;
                }
            };

            let http_client = self.http_client.clone();
            let mint_pubkey = schedule.mint_pubkey.clone();
            let current_db_count = db_counts_map.get(&mint_pubkey).copied().unwrap_or(0);
            let last_holder_count = schedule.last_holder_count;

            let handle = tokio::spawn(async move {
                let _permit = permit;

                // todo!: 算了，暂时先直接通过api获取吧，后续在进行其他的考虑
                // let onchain_result = http_client.get_token_holders(&mint_pubkey).await;
                let onchain_result = http_client.get_sol_scan_holder(&mint_pubkey).await;

                ReconciliationTaskResult {
                    mint_pubkey,
                    db_holder_count: current_db_count,
                    last_holder_count,
                    onchain_result,
                }
            });

            handles.push(handle);
        }

        futures::future::join_all(handles).await
    }

    /// 判断是否需要重建
    fn should_rebuild(
        &self,
        db_count: i64,
        onchain_count: i64,
        config: &ReconciliationConfig,
    ) -> bool {
        if db_count == 0 {
            // 数据库为空但链上有 holder，需要重建
            return onchain_count > 0;
        }

        // 正常计算差异百分比
        let difference = (db_count - onchain_count).abs();
        let difference_percentage = difference * 100 / db_count;
        difference_percentage > config.max_difference_percent as i64
    }

    /// 执行重建和追赶流程
    async fn rebuild_and_catchup(&self, mint_pubkey: &str) -> Result<()> {
        app_info!("Starting rebuild for mint {} due to significant difference", mint_pubkey);

        // 重新获取完整的 holder 列表用于重建
        let holders = self.http_client.get_token_holders(mint_pubkey).await?;

        if holders.is_empty() {
            app_warn!("Got empty holders list for mint {}, skipping rebuild", mint_pubkey);
            return Ok(());
        }

        // 建立 baseline
        let baseline_slot = self.database
            .establish_baseline_atomic(mint_pubkey, holders)
            .await?;

        app_info!("Rebuilt baseline for mint {} at slot {}", mint_pubkey, baseline_slot);

        // 执行 catch-up
        self.catch_up(baseline_slot, mint_pubkey).await?;

        app_info!("✅ Successfully rebuilt and caught up for mint {}", mint_pubkey);
        Ok(())
    }

    /// 处理单个对账结果
    async fn process_single_reconciliation_result(
        &self,
        task_result: ReconciliationTaskResult,
        config: &ReconciliationConfig,
    ) -> Result<()> {
        match task_result.onchain_result {
            Ok(holders_count) => {
                let holders_count = holders_count as i64;

                // 检查是否需要重建
                let needs_rebuild = self.should_rebuild(
                    task_result.db_holder_count,
                    holders_count,
                    config,
                );

                if needs_rebuild {
                    let difference = (task_result.db_holder_count - holders_count).abs();

                    if task_result.db_holder_count == 0 {
                        app_warn!(
                            "Token:{} has {} holders onchain but 0 in db, needs rebuild",
                            &task_result.mint_pubkey,
                            holders_count
                        );
                    } else {
                        let difference_percentage = difference * 100 / task_result.db_holder_count;
                        app_error!(
                            "Token:{} count in db {} is not same as onchain count {}, difference: {}%",
                            &task_result.mint_pubkey,
                            task_result.db_holder_count,
                            holders_count,
                            difference_percentage
                        );
                    }

                    // 执行重建流程
                    if let Err(e) = self.rebuild_and_catchup(&task_result.mint_pubkey).await {
                        app_error!("Failed to rebuild mint {}: {}", task_result.mint_pubkey, e);
                    }
                }

                // 根据数据库持有者数量变化更新对账计划
                match self.update_schedule_based_on_db_change(
                    &task_result.mint_pubkey,
                    task_result.db_holder_count,
                    task_result.last_holder_count,
                ).await {
                    Ok(next_interval_hours) => {
                        app_info!(
                            "✅ Reconciliation completed for mint {}: onchain={}, db={}, next_interval={}h",
                            task_result.mint_pubkey,
                            holders_count,
                            task_result.db_holder_count,
                            next_interval_hours
                        );
                    }
                    Err(e) => {
                        app_error!("Failed to update schedule for mint {}: {}", task_result.mint_pubkey, e);
                    }
                }
            }
            Err(e) => {
                // RPC 调用失败，仍然更新 schedule（基于数据库变化）
                app_warn!("Failed to get onchain data for mint {}: {}", task_result.mint_pubkey, e);

                if let Err(e) = self.update_schedule_based_on_db_change(
                    &task_result.mint_pubkey,
                    task_result.db_holder_count,
                    task_result.last_holder_count,
                ).await {
                    app_error!("Failed to update schedule for mint {}: {}", task_result.mint_pubkey, e);
                }
            }
        }

        Ok(())
    }

    /// 处理一批对账任务
    async fn process_reconciliation_batch(
        &self,
        due_mints: Vec<crate::database::repositories::reconciliation_schedule::ReconciliationSchedule>,
        config: &ReconciliationConfig,
    ) -> Result<()> {
        let total_mints = due_mints.len();
        app_info!("Found {} mints due for reconciliation", total_mints);

        // 1. 提取所有 mint_pubkeys
        let mint_pubkeys: Vec<String> = due_mints
            .iter()
            .map(|schedule| schedule.mint_pubkey.clone())
            .collect();

        // 2. 批量查询当前数据库中的 holder_count
        let db_counts_map = self.fetch_db_holder_counts(&mint_pubkeys).await?;

        // 3. 并发获取链上数据
        app_info!("Waiting for all reconciliation tasks to complete (timeout: {}s)", config.timeout_seconds);

        let timeout_duration = tokio::time::Duration::from_secs(config.timeout_seconds);
        let results = match tokio::time::timeout(
            timeout_duration,
            self.fetch_onchain_data_concurrently(due_mints, &db_counts_map, config)
        ).await {
            Ok(results) => results,
            Err(_) => {
                app_error!(
                    "Reconciliation batch timeout after {}s for {} mints, skipping this batch",
                    config.timeout_seconds,
                    total_mints
                );
                return Ok(());
            }
        };

        // 4. 处理结果
        for result in results {
            match result {
                Ok(task_result) => {
                    if let Err(e) = self.process_single_reconciliation_result(task_result, config).await {
                        app_error!("Failed to process reconciliation result: {}", e);
                    }
                }
                Err(e) => {
                    app_error!("Task panicked: {}", e);
                }
            }
        }

        Ok(())
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