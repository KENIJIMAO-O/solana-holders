use std::collections::HashMap;
use crate::database::repositories::TokenAccountAggregateData;
use crate::clickhouse::clickhouse::Event;
use crate::clickhouse::helper::ClickhouseDecimal;

/// 聚合相同 account_pubkey 的 events，避免 ON CONFLICT 多次更新同一行
pub fn aggregate_token_account_events(events: &[Event]) -> Vec<TokenAccountAggregateData> {
    // Key: account_pubkey, Value: (mint, owner, aggregated_delta, max_slot)
    let mut aggregation_map: HashMap<String, (String, String, ClickhouseDecimal, u64)> = HashMap::new();

    for event in events {
        aggregation_map
            .entry(event.account_pubkey.clone())
            .and_modify(|(_, _, delta, slot)| {
                *delta = *delta + event.delta;
                *slot = (*slot).max(event.slot);
            })
            .or_insert((
                event.mint_pubkey.clone(),
                event.owner_pubkey.clone(),
                event.delta,
                event.slot,
            ));
    }

    aggregation_map
        .into_iter()
        .map(
            |(acct, (mint, owner, delta, slot))| TokenAccountAggregateData {
                acct_pubkey: acct,
                mint_pubkey: mint,
                owner_pubkey: owner,
                delta,
                last_updated_slot: slot as i64,
            },
        )
        .collect()
}
