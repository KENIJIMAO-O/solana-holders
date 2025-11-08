use crate::EVENT_LOG_TARGET;
use crate::message_queue::token_event_message_queue::Redis;
use crate::monitor::client::GrpcClient;
use crate::monitor::utils::constant::{TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID_2022};
use crate::monitor::utils::utils::{
    convert_to_encoded_tx, subtract_as_decimal, txn_signature_to_string,
};
use crate::utils::timer::TaskLogger;
use anyhow::{Error, anyhow};
use chrono::Local;
use futures::SinkExt;
use futures::future::join_all;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status_client_types::EncodedTransactionWithStatusMeta;
use solana_transaction_status_client_types::option_serializer::OptionSerializer;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestPing};
use yellowstone_grpc_proto::tonic::codegen::tokio_stream::StreamExt;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum InstructionType {
    Transfer(Pubkey, usize, Pubkey, usize, u64), // source, dest, amount
    TransferChecked(Pubkey, Pubkey, u64, u8),    // source, dest, amount, decimal
    MintTo(),
    MintToChecked(),
    Burn(),
    BurnChecked(),
    Other,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TokenEvent {
    // 唯一标识一个指令
    pub slot: u64,
    pub tx_signature: String,
    pub instruction_index: u32,

    // 代币核心信息
    pub mint_address: Pubkey,
    pub account_address: Pubkey,
    pub owner_address: Option<Pubkey>,

    // 余额变化
    pub delta: String,

    pub instruction_type: InstructionType,

    // 处理状态
    pub confirmed: bool,
}

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub commitment: CommitmentLevel,
}

impl MonitorConfig {
    pub fn new() -> Self {
        let commitment = env::var("COMMITMENT").unwrap_or_else(|_| "Finalized".to_string());
        let commitment_level = match commitment.as_str() {
            "Processed" => CommitmentLevel::Processed,
            "Confirmed" => CommitmentLevel::Confirmed,
            "Finalized" => CommitmentLevel::Finalized,
            _ => CommitmentLevel::Finalized,
        };

        Self {
            commitment: commitment_level,
        }
    }
}

#[derive(Debug)]
pub struct ReConnectConfig {
    pub(crate) reconnect_count: AtomicU32,   // 当前的重连次数
    pub(crate) max_reconnect_attempts: u32,  // 最大重连次数
    pub(crate) initial_backoff_seconds: u64, // 初始重连间隔
    pub(crate) max_backoff_seconds: u64,     // 最大重连间隔
}

impl Default for ReConnectConfig {
    fn default() -> Self {
        Self {
            reconnect_count: AtomicU32::new(0),
            max_reconnect_attempts: 5,
            initial_backoff_seconds: 1,
            max_backoff_seconds: 300,
        }
    }
}

#[derive(Debug)]
pub struct Monitor {
    config: MonitorConfig,
    client: GrpcClient,
    message_queue: Arc<Redis>,
    reconnect_config: ReConnectConfig,
}

impl Monitor {
    pub fn new(
        config: MonitorConfig,
        client: GrpcClient,
        message_queue: Arc<Redis>,
        reconnect_config: ReConnectConfig,
    ) -> Self {
        Self {
            config,
            client,
            message_queue,
            reconnect_config,
        }
    }

    pub async fn run_with_reconnect(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<(), Error> {
        info!("Monitor starting with auto-reconnect capability");

        loop {
            // 检查取消信号
            if cancellation_token.is_cancelled() {
                info!("Monitor cancelled before attempting connection");
                break;
            }

            let reconnect_count = self.reconnect_config.reconnect_count.load(Ordering::SeqCst);

            // 检查是否超过最大重连次数
            if reconnect_count >= self.reconnect_config.max_reconnect_attempts {
                error!(
                    "Maximum reconnection attempts ({}) exceeded, stopping monitor",
                    self.reconnect_config.max_reconnect_attempts
                );
                return Err(anyhow!("Maximum reconnection attempts exceeded"));
            }

            // 如果不是第一次连接，需要等待退避时间
            if reconnect_count > 0 {
                let backoff_seconds = (self.reconnect_config.initial_backoff_seconds
                    * 2_u64.pow(reconnect_count.saturating_sub(1)))
                .min(self.reconnect_config.max_backoff_seconds);

                info!(
                    "Reconnection attempt {} after {}s delay",
                    reconnect_count + 1,
                    backoff_seconds
                );

                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("Monitor cancelled during reconnection backoff");
                        break;
                    }
                    _ = sleep(Duration::from_secs(backoff_seconds)) => {
                    }
                }
            }

            // 尝试连接和处理
            match self.run_single_connection(cancellation_token.clone()).await {
                Ok(_) => {
                    info!("Monitor connection ended gracefully");
                    break;
                }
                Err(e) => {
                    let current_count = self
                        .reconnect_config
                        .reconnect_count
                        .fetch_add(1, Ordering::SeqCst);
                    error!("Monitor connection error:{:?}", e);
                    // todo: 分类错误类型
                    ()
                }
            }
        }

        info!("Monitor stopped gracefully");
        Ok(())
    }

    async fn run_single_connection(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> Result<(), Error> {
        let token_program = TOKEN_PROGRAM_ID.to_string();
        let token_program_2022 = TOKEN_PROGRAM_ID_2022.to_string();
        let targets = vec![token_program, token_program_2022];
        info!(
            "Monitor connecting with token_program: {:?}, commitment: {:?}",
            targets.clone(),
            self.config.commitment
        );

        let (mut subscribe_tx, mut stream) = self
            .client
            .subscribe_block(targets, Some(true), None, self.config.commitment)
            .await?;

        info!("Monitor subscription established, processing blocks");

        self.reconnect_config
            .reconnect_count
            .store(0, Ordering::SeqCst);

        // 启动 gRPC 连接监控任务（通过流状态监控）
        let connection_monitor = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                debug!("🔗 gRPC connection monitor: stream active");
            }
        });

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Monitor processing cancelled");
                    connection_monitor.abort();
                    break;
                }
                data = stream.next() => {
                    match data {
                        Some(Ok(data))=> {
                            if let Some(update) = data.update_oneof{
                                match update {
                                    UpdateOneof::Ping(_) => {
                                        let _ = subscribe_tx
                                            .send(SubscribeRequest {
                                            ping: Some(SubscribeRequestPing { id: 1 }),
                                            ..Default::default()
                                            })
                                            .await;
                                        debug!("service is ping: {:#?}", Local::now());
                                    }
                                    UpdateOneof::Pong(_) => {
                                        debug!("service is pong: {:#?}", Local::now());
                                    }
                                    UpdateOneof::Block(sub_block) => {
                                        let slot = sub_block.slot;
                                        info!("📥 收到 Slot {}", slot);

                                        let message_queue = Arc::clone(&self.message_queue);
                                        // 这里可能要添加线程控制
                                        tokio::spawn(async move {
                                            let start = std::time::Instant::now();

                                            if let Err(e) = Self::process_block_static(sub_block, message_queue).await {
                                                error!("Failed to process block {}: {}", slot, e);
                                            }

                                            let elapsed = start.elapsed();
                                            info!("🕐 Slot {} 总耗时: {:?}", slot, elapsed);
                                        });
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!("Stream error: {:?}", e);
                            connection_monitor.abort();
                            return Err(anyhow!("Stream error: {}", e));
                        }
                        None => {
                            warn!("Monitor stream ended unexpectedly");
                            connection_monitor.abort();
                            return Err(anyhow!("Stream ended unexpectedly"));
                        }
                    }

                }

            }
        }

        Ok(())
    }

    // 处理整个 block，收集所有事件并批量入队
    // todo!: 要设计一个好的入队算法
    async fn process_block(
        &self,
        sub_block: yellowstone_grpc_proto::geyser::SubscribeUpdateBlock,
    ) -> Result<(), Error> {
        Self::process_block_static(sub_block, Arc::clone(&self.message_queue)).await
    }

    // 静态版本，用于 spawn
    async fn process_block_static(
        sub_block: yellowstone_grpc_proto::geyser::SubscribeUpdateBlock,
        message_queue: Arc<Redis>,
    ) -> Result<(), Error> {
        let block_slot = sub_block.slot;
        let tx_count = sub_block.transactions.len();
        info!("📦 Slot {}: 开始处理 {} 笔交易", block_slot, tx_count);

        let mut monitor_logger = TaskLogger::new("monitor logger", "1");

        monitor_logger.log("start to handle whole txs in a slot");
        // 并发处理所有交易
        let transactions = sub_block.transactions; // 将所有权移出

        // 只创建一个 spawn_blocking 任务，将整个并行计算包裹起来
        // 使用 Rayon 的 into_par_iter() 来并行处理 transactions
        let all_events = tokio::task::spawn_blocking(move || {
            transactions
                .into_par_iter()
                .enumerate()
                .flat_map(|(tx_index, tx)| {
                    let sig = txn_signature_to_string(tx.signature.clone())
                        .unwrap_or_else(|| format!("unknown_{}", tx_index));

                    convert_to_encoded_tx(tx)
                        .ok()
                        .and_then(|encoded_tx| {
                            Self::process_transaction(encoded_tx, sig, block_slot).ok()
                        })
                        .unwrap_or_default() // Option<Vec<TokenEvent>> -> Vec<TokenEvent>
                })
                .collect::<Vec<TokenEvent>>()
        })
        .await?;
        let target_instruction_count = all_events.len();

        // 批量发送到消息队列
        monitor_logger.log("start to push events to message queue");
        if !all_events.is_empty() {
            message_queue
                .batch_enqueue_holder_event(all_events, &mut monitor_logger)
                .await?;
        }

        info!(
            "✅ Slot {} 处理完成: 总交易={}, 目标指令={}",
            block_slot, tx_count, target_instruction_count
        );
        Ok(())
    }

    async fn send_events_to_message_queue(
        &self,
        all_events: Vec<TokenEvent>,
        monitor_logger: &mut TaskLogger,
    ) -> anyhow::Result<()> {
        if !all_events.is_empty() {
            self.message_queue
                .batch_enqueue_holder_event(all_events, monitor_logger)
                .await?;
        }
        Ok(())
    }

    #[instrument(skip_all)]
    fn process_transaction(
        transaction: EncodedTransactionWithStatusMeta,
        sig: String,
        block_slot: u64,
    ) -> Result<Vec<TokenEvent>, Error> {
        let meta = transaction
            .meta
            .as_ref()
            .ok_or_else(|| anyhow!("无 Meta 数据"))?;
        debug!(target: EVENT_LOG_TARGET, "slot:{}, sig:{:?}", block_slot, sig);

        // 判断当前交易是否成功(如果失败，不做任何动作直接返回)
        if meta.err.is_some() {
            return Ok(Vec::new());
        }

        let tx = transaction
            .transaction
            .decode()
            .ok_or_else(|| anyhow!("无法解码交易"))?;

        // 组装当前交易所有 account_keys
        let mut account_keys = tx.message.static_account_keys().to_vec();

        // 如果有 loaded_addresses，就追加到 account_keys
        if meta.loaded_addresses.is_some() {
            let loaded_address = meta.loaded_addresses.as_ref().unwrap();

            // 获取可写和只读动态账户
            let write_address = &loaded_address
                .writable
                .iter()
                .map(|addr| Pubkey::from_str_const(&addr))
                .collect::<Vec<_>>();
            let read_address = &loaded_address
                .readonly
                .iter()
                .map(|addr| Pubkey::from_str_const(&addr))
                .collect::<Vec<_>>();

            account_keys.extend(write_address);
            account_keys.extend(read_address);
        }

        let instructions = tx.message.instructions();
        let inner_instructions = match &meta.inner_instructions {
            OptionSerializer::Some(inner_ixs) => Some(inner_ixs),
            _ => None,
        };

        debug!(
            "分析交易，包含 {} 个外部指令，{} 个内部指令组",
            instructions.len(),
            inner_instructions.as_ref().map_or(0, |ixs| ixs.len())
        );

        let mut events = Vec::new();

        // 只有token变化的交易，才有可能改变holder数量
        if let (OptionSerializer::Some(pre_balances), OptionSerializer::Some(post_balances)) =
            (&meta.pre_token_balances, &meta.post_token_balances)
        {
            // 创建account_index -> post_balance的映射
            let pre_balance_map: HashMap<u8, _> =
                pre_balances.iter().map(|b| (b.account_index, b)).collect();
            let post_balance_map: HashMap<u8, _> =
                post_balances.iter().map(|b| (b.account_index, b)).collect();

            let mut instruction_index = 0u32;

            // 处理pre和post都存在代币账户的情况，直接遍历所有pre_balance，看是否有变化
            for pre_balance in pre_balances {
                match post_balance_map.get(&pre_balance.account_index) {
                    // 处理pre和post都存在的情况
                    Some(post_balance) => {
                        // 检查余额是否有变化
                        if pre_balance.ui_token_amount.ui_amount_string
                            != post_balance.ui_token_amount.ui_amount_string
                        {
                            let delta = subtract_as_decimal(
                                &post_balance.ui_token_amount.ui_amount_string,
                                &pre_balance.ui_token_amount.ui_amount_string,
                            )?;

                            let owner = match &post_balance.owner {
                                OptionSerializer::Some(owner) => {
                                    Some(Pubkey::from_str_const(owner))
                                }
                                _ => None,
                            };

                            // 关键：通过account_index获取真实的token account地址
                            let token_account = *account_keys
                                .get(pre_balance.account_index as usize)
                                .ok_or_else(|| {
                                    anyhow!("Invalid account_index: {}", pre_balance.account_index)
                                })?;

                            let token_event = TokenEvent {
                                slot: block_slot,
                                tx_signature: sig.clone(),
                                instruction_index,
                                mint_address: Pubkey::from_str_const(&pre_balance.mint),
                                account_address: token_account,
                                owner_address: owner,
                                delta,
                                instruction_type: InstructionType::Other, // 简化，不关注具体类型
                                confirmed: false, // 0 as false
                            };
                            events.push(token_event);
                            instruction_index += 1;
                        }
                    }
                    None => {
                        // 情况3: 只在 Pre 中存在 - CloseAccount，余额归零
                        // Delta = 0 - pre_balance (负数)
                        let pre_amount = &pre_balance.ui_token_amount.ui_amount_string;

                        // 只有当pre余额不为0时才记录
                        if pre_amount != "0" {
                            let delta = format!("-{}", pre_amount);

                            let owner = match &pre_balance.owner {
                                OptionSerializer::Some(owner) => {
                                    Some(Pubkey::from_str_const(owner))
                                }
                                _ => None,
                            };

                            let token_account = *account_keys
                                .get(pre_balance.account_index as usize)
                                .ok_or_else(|| {
                                    anyhow!("Invalid account_index: {}", pre_balance.account_index)
                                })?;

                            let token_event = TokenEvent {
                                slot: block_slot,
                                tx_signature: sig.clone(),
                                instruction_index,
                                mint_address: Pubkey::from_str_const(&pre_balance.mint),
                                account_address: token_account,
                                owner_address: owner,
                                delta,
                                instruction_type: InstructionType::Other,
                                confirmed: false,
                            };
                            events.push(token_event);
                            instruction_index += 1;
                        }
                    }
                }
            }

            // 情况2：遍历所有 post_balances，找只在 Post 中存在的账户
            // 处理: Post - Pre (新创建的账户或首次接收token)
            for post_balance in post_balances {
                if !pre_balance_map.contains_key(&post_balance.account_index) {
                    // 只在 Post 中存在 - 新账户，余额从0增加
                    // Delta = post_balance - 0 (正数)
                    let post_amount = &post_balance.ui_token_amount.ui_amount_string;

                    // 只有当post余额不为0时才记录
                    if post_amount != "0" {
                        let delta = post_amount.clone();

                        let owner = match &post_balance.owner {
                            OptionSerializer::Some(owner) => Some(Pubkey::from_str_const(owner)),
                            _ => None,
                        };

                        let token_account = *account_keys
                            .get(post_balance.account_index as usize)
                            .ok_or_else(|| {
                                anyhow!("Invalid account_index: {}", post_balance.account_index)
                            })?;

                        let token_event = TokenEvent {
                            slot: block_slot,
                            tx_signature: sig.clone(),
                            instruction_index,
                            mint_address: Pubkey::from_str_const(&post_balance.mint),
                            account_address: token_account,
                            owner_address: owner,
                            delta,
                            instruction_type: InstructionType::Other,
                            confirmed: false,
                        };
                        events.push(token_event);
                        instruction_index += 1;
                    }
                }
            }
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_queue::token_event_message_queue::RedisQueueConfig;

    #[tokio::test]
    async fn test_monitor() {
        dotenv::dotenv().ok();
        let monitor_config = MonitorConfig::new();
        let rpc_url = env::var("RPC_URL").unwrap();
        let client = GrpcClient::new(&rpc_url);

        // 创建消息队列
        let redis_url = env::var("REDIS_URL");
        let config = RedisQueueConfig::default();
        let message_queue = Redis::new(&redis_url.unwrap(), config).await.unwrap();
        let _ = message_queue.initialize_message_queue().await.unwrap();

        let re_connect_config = ReConnectConfig::default();

        let mut onchain_monitor = Monitor::new(
            monitor_config,
            client,
            Arc::new(message_queue),
            re_connect_config,
        );

        let cancellation_token = CancellationToken::new();
        let token = cancellation_token.child_token();

        let result = onchain_monitor.run_with_reconnect(token).await;
    }

    /// 测试纯粹的gRPC slot接收速度，不做任何处理
    /// 用于排查是gRPC本身慢还是处理逻辑慢
    #[tokio::test]
    async fn test_grpc_slot_receive_speed() {
        dotenv::dotenv().ok();
        let monitor_config = MonitorConfig::new();
        let rpc_url = env::var("GRPC_URL").unwrap();
        let client = GrpcClient::new(&rpc_url);

        let token_program = TOKEN_PROGRAM_ID.to_string();
        let token_program_2022 = TOKEN_PROGRAM_ID_2022.to_string();
        let targets = vec![token_program, token_program_2022];

        println!("🔌 正在连接 gRPC...");
        let (mut subscribe_tx, mut stream) = client
            .subscribe_block(targets, Some(true), None, monitor_config.commitment)
            .await
            .unwrap();

        println!("✅ gRPC 连接成功，开始接收 slot（只打印，不做任何处理）");
        println!("📊 Commitment: {:?}", monitor_config.commitment);

        let mut last_slot = 0u64;

        loop {
            if let Some(Ok(data)) = stream.next().await {
                if let Some(update) = data.update_oneof {
                    match update {
                        UpdateOneof::Ping(_) => {
                            let _ = subscribe_tx
                                .send(SubscribeRequest {
                                    ping: Some(SubscribeRequestPing { id: 1 }),
                                    ..Default::default()
                                })
                                .await;
                        }
                        UpdateOneof::Block(sub_block) => {
                            let now = Local::now().format("%H:%M:%S%.3f");
                            let slot = sub_block.slot;
                            let slot_diff = if last_slot > 0 {
                                slot.saturating_sub(last_slot)
                            } else {
                                0
                            };

                            if slot_diff > 1 {
                                println!(
                                    "[{}] ⚠️  Slot {} (跳过了 {} 个slot)",
                                    now,
                                    slot,
                                    slot_diff - 1
                                );
                            } else {
                                println!("[{}] 📥 Slot {}", now, slot);
                            }

                            last_slot = slot;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
