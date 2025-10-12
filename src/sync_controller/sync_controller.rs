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

    pub async fn start(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 1000;
        const BATCH_TIMEOUT_MS: usize = 100;
        let consumer_name = "sync_dequeuer";

        loop {
            let mut logger = TaskLogger::new("sync controller", "2");

            if cancellation_token.is_cancelled() {
                info!("SyncController cancelled");
                break;
            }

            let datas = match self
                .redis
                .batch_dequeue_holder_event(
                    consumer_name,
                    BATCH_SIZE,
                    BATCH_TIMEOUT_MS,
                    &mut logger,
                )
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    error!("Failed to dequeue from Redis: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            if datas.is_empty() {
                // 队列为空时短暂休眠，避免空转消耗 CPU
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            logger.log("batch dequeue holder events complete");

            // 使用 unzip 将一个元组迭代器拆分成两个集合
            let (message_ids, token_events): (Vec<String>, Vec<Event>) = datas
                .into_iter()
                .map(|(message_id, token_event)| {
                    // 1. 尝试将 delta 字符串解析为 Decimal
                    let delta = match token_event.delta.parse::<Decimal>() {
                        Ok(d) => d, // 如果成功，得到 Decimal 值
                        Err(e) => {
                            // 如果失败，打印日志并返回 None，这条记录将被丢弃
                            info!(
                                "Skipping event with invalid delta. Tx: {}, Delta: '{}', Error: {}",
                                token_event.tx_signature, token_event.delta, e
                            );
                            // return None; // 关键：返回 None 来过滤掉此项
                            Decimal::from(1)
                        }
                    };

                    // 2. 如果 delta 解析成功，继续构建 Event 对象
                    let event = match token_event.owner_address {
                        None => Event::new(
                            token_event.slot,
                            token_event.tx_signature,
                            token_event.mint_address.to_string(),
                            token_event.account_address.to_string(),
                            "".to_string(),
                            delta, 
                            token_event.confirmed,
                        ),
                        Some(owner_pubkey) => Event::new(
                            token_event.slot,
                            token_event.tx_signature,
                            token_event.mint_address.to_string(),
                            token_event.account_address.to_string(),
                            owner_pubkey.to_string(),
                            delta, 
                            token_event.confirmed,
                        ),
                    };

                    // 3. 将有效的元组包裹在 Some 中返回
                    (message_id, event)
                })
                .unzip();

            // 插入到数据库
            if !token_events.is_empty() {
                let event_count = token_events.len();
                if let Err(e) = self
                    .database
                    .upsert_events_btach(&token_events, &mut logger)
                    .await
                {
                    error!("Error upserting events: {}", e);
                    continue;
                }
                logger.log("sql upsert events complete");
                // 数据库更改成功之后才能ack
                if let Err(e) = self.redis.ack_messages(&message_ids).await {
                    error!("Error acknowledging messages: {}", e);
                    continue;
                }
                logger.log("redis ack messages complete");
                info!("✅ SyncController: 处理 {} 个事件", event_count);
            }
        }
        Ok(())
    }

    pub async fn build_baseline(&self, mint: &str) -> anyhow::Result<()> {
        // todo!: 构建baseline的时候只需要插入进行，后续监听合并的过程会有对token account balance的大量更新，而不是插入
        let token_accounts = self.http_client.get_token_holders(mint).await?;

        let mut logger = TaskLogger::new("baseline", "3");

        if !token_accounts.is_empty() {
            if let Err(e) = self
                .database
                .upsert_token_accounts_batch(&token_accounts, &mut logger)
                .await
            {
                error!("Error upserting events into token accounts: {}", e);
            }

            let holder_account = match self
                .database
                .upsert_holders_batch(&token_accounts, &mut logger)
                .await
            {
                Ok(h) => h,
                Err(e) => {
                    error!("Error upserting events into holders: {}", e);
                    return Err(anyhow::anyhow!(e));
                }
            };

            let last_updated_slot = token_accounts[0].slot;

            if let Err(e) = self
                .database
                .upsert_mint_stats(mint, holder_account as i64, last_updated_slot as i64)
                .await
            {
                error!("Error upserting events into mint stats: {}", e);
            }
        }
        Ok(())
    }
}
