use crate::message_queue::token_event_message_queue::Redis;
use crate::monitor::client::GrpcClient;
use crate::monitor::utils::constant::{TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID_2022};
use crate::monitor::utils::utils::{
    convert_to_encoded_tx, subtract_as_decimal, txn_signature_to_string,
};
use crate::utils::timer::TaskLogger;
use anyhow::{Error, anyhow};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status_client_types::EncodedTransactionWithStatusMeta;
use solana_transaction_status_client_types::option_serializer::OptionSerializer;
use spl_token::instruction::TokenInstruction;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
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
    pub slot: i64,
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

#[derive(Debug, Clone)]
pub struct ReConnectConfig {
    pub reconnect_count: u8,      // 当前的重连次数
    max_reconnect_attempts: u8,   // 最大重连次数
    initial_backoff_seconds: u16, // 初始重连间隔
    max_backoff_seconds: u16,     // 最大重连间隔
}

impl Default for ReConnectConfig {
    fn default() -> Self {
        Self {
            reconnect_count: 0,
            max_reconnect_attempts: 5,
            initial_backoff_seconds: 1,
            max_backoff_seconds: 300,
        }
    }
}

#[derive(Debug, Clone)]
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

            let reconnect_count = self.reconnect_config.reconnect_count;

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
                    * 2_u16.pow(reconnect_count.saturating_sub(1) as u32))
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
                    _ = sleep(Duration::from_secs(backoff_seconds as u64)) => {
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
                    self.reconnect_config.reconnect_count += 1;

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

        self.reconnect_config.reconnect_count = 0;

        // 启动 gRPC 连接监控任务（通过流状态监控）
        let connection_monitor = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                // let _ = subscribe_tx
                //     .send(SubscribeRequest {
                //         ping: Some(SubscribeRequestPing { id: 1 }),
                //         ..Default::default()
                //     })
                //     .await;
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
                                    UpdateOneof::Block(sub_block) => {
                                        if let Err(e) = self.process_block(sub_block).await {
                                            error!("Failed to process block: {}", e);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Some(Err(e)) => {

                        }
                        None => {}
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
        let block_slot = sub_block.slot;
        let tx_count = sub_block.transactions.len();
        let mut monitor_logger = TaskLogger::new("monitor logger", "1");

        monitor_logger.log("start to handle whole txs in a slot");
        // 并发处理所有交易
        let tasks: Vec<_> = sub_block
            .transactions
            .into_iter()
            .enumerate()
            .map(|(tx_index, tx)| {
                let sig = txn_signature_to_string(tx.signature.clone()).unwrap_or_else(|| {
                    warn!("Failed to parse transaction signature");
                    format!("unknown_{}", tx_index)
                });

                tokio::spawn(async move {
                    convert_to_encoded_tx(tx).ok().and_then(|encoded_tx| {
                        // 使用 block_on 同步执行异步函数
                        futures::executor::block_on(Self::process_transaction(
                            encoded_tx,
                            sig.clone(),
                            block_slot as i64,
                        ))
                        .ok()
                    })
                })
            })
            .collect();

        // 等待所有任务完成并收集事件
        let results = join_all(tasks).await;
        let mut all_events = Vec::new();

        for result in results {
            if let Ok(Some(events)) = result {
                all_events.extend(events);
            }
        }
        let target_tx_count = all_events.len();

        // 批量发送到消息队列
        monitor_logger.log("start to push events to message queue");
        self.send_events_to_message_queue(all_events, &mut monitor_logger)
            .await?;

        info!(
            "✅ Slot {} 处理完成: 总交易={}, 目标事件={}",
            block_slot, tx_count, target_tx_count
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
    async fn process_transaction(
        transaction: EncodedTransactionWithStatusMeta,
        sig: String,
        block_slot: i64,
    ) -> Result<Vec<TokenEvent>, Error> {
        let meta = transaction
            .meta
            .as_ref()
            .ok_or_else(|| anyhow!("无 Meta 数据"))?;

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
            let post_balance_map: HashMap<u8, _> =
                post_balances.iter().map(|b| (b.account_index, b)).collect();

            let mut instruction_index = 0u32;

            // 直接遍历所有pre_balance，看是否有变化
            for pre_balance in pre_balances {
                if let Some(post_balance) = post_balance_map.get(&pre_balance.account_index) {
                    // 检查余额是否有变化
                    if pre_balance.ui_token_amount.ui_amount_string
                        != post_balance.ui_token_amount.ui_amount_string
                    {
                        let delta = subtract_as_decimal(
                            &post_balance.ui_token_amount.ui_amount_string,
                            &pre_balance.ui_token_amount.ui_amount_string,
                        )?;

                        let owner = match &pre_balance.owner {
                            OptionSerializer::Some(owner) => Some(Pubkey::from_str_const(owner)),
                            _ => None,
                        };

                        // 关键：通过account_index获取真实的token account地址
                        let token_account = account_keys[pre_balance.account_index as usize];

                        let token_event = TokenEvent {
                            slot: block_slot,
                            tx_signature: sig.clone(),
                            instruction_index,
                            mint_address: Pubkey::from_str_const(&pre_balance.mint),
                            account_address: token_account,
                            owner_address: owner,
                            delta,
                            instruction_type: InstructionType::Other, // 简化，不关注具体类型
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

    pub fn proccess_instruction(
        ix: &CompiledInstruction,
        account_keys: &Vec<Pubkey>,
    ) -> Result<InstructionType, Error> {
        if (ix.program_id_index as usize) > account_keys.len() {
            return Err(anyhow!("Program ID index out of bounds"));
        }

        let program_id = account_keys[ix.program_id_index as usize];
        if !program_id.eq(&TOKEN_PROGRAM_ID) && !program_id.eq(&TOKEN_PROGRAM_ID_2022) {
            return Err(anyhow!("Wrong program ID"));
        }

        if ix.data.is_empty() {
            return Err(anyhow!("Empty data"));
        }

        let account_indexs = &ix.accounts;
        match TokenInstruction::unpack(&ix.data) {
            Ok(TokenInstruction::Transfer { amount }) => {
                let source_account_index = account_indexs[0] as usize;
                let destination_account_index = account_indexs[1] as usize;

                let source_account = account_keys[source_account_index];
                let destination_account = account_keys[destination_account_index];
                Ok(InstructionType::Transfer(
                    source_account,
                    source_account_index,
                    destination_account,
                    destination_account_index,
                    amount,
                ))
            }
            Ok(TokenInstruction::TransferChecked { amount, decimals }) => {
                let source_account = account_keys[account_indexs[0] as usize];
                let destination_account = account_keys[account_indexs[2] as usize];
                Ok(InstructionType::TransferChecked(
                    source_account,
                    destination_account,
                    amount,
                    decimals,
                ))
            }
            Ok(TokenInstruction::MintTo { amount }) => Ok(InstructionType::MintTo()),
            Ok(TokenInstruction::MintToChecked {
                amount: u64,
                decimals: u8,
            }) => Ok(InstructionType::MintToChecked()),
            Ok(TokenInstruction::Burn { amount }) => Ok(InstructionType::Burn()),
            Ok(TokenInstruction::BurnChecked {
                amount: u64,
                decimals: u8,
            }) => Ok(InstructionType::BurnChecked()),
            _ => Ok(InstructionType::Other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::baseline::getProgramAccounts::HttpClient;
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
}
