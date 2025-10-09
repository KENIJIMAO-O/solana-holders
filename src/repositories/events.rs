use anyhow::{Result, Error};
use yellowstone_grpc_proto::tonic::async_trait;
use crate::database::postgresql::DatabaseConnection;

pub struct Event {
    slot: u64,
    tx_sig: String,
    mint_pubkey: String,
    account_pubkey: String,
    owner_pubkey: String,
    delta: u64,
    confirmed: bool
}

impl Event {
    pub fn new(
        slot: u64,
        tx_sig: String,
        mint_pubkey: String,
        account_pubkey: String,
        owner_pubkey: String,
        delta: u64,
        confirmed: bool
    ) -> Self {
        Self {
            slot,
            tx_sig,
            mint_pubkey,
            account_pubkey,
            owner_pubkey,
            delta,
            confirmed,
        }
    }
}

#[async_trait]
pub trait EventsRepository {
    /// 批量插入或者更新
    async fn upsert_events_btach(&self, events: &[Event]) -> Result<(), Error>;

    /// 更新confirmed
    async fn confirm_events(&self, events: &[Event]) -> Result<(), Error>;
    // todo!:更多的repo
}

#[async_trait]
impl EventsRepository for DatabaseConnection {
    async fn upsert_events_btach(&self, events: &[Event]) -> Result<(), Error> {
        if events.is_empty() {
            return Ok(());
        }

        // 在这个连接上开启事务
        let mut tx = self.pool.begin().await?;

        let slots:Vec<i64> = events
            .iter()
            .map(|event| {event.slot as i64})
            .collect::<Vec<_>>();
        let tx_sigs = events
            .iter()
            .map(|event| {event.tx_sig.clone()})
            .collect::<Vec<_>>();
        let mint_pubkeys = events
            .iter()
            .map(|event| {event.mint_pubkey.clone()})
            .collect::<Vec<_>>();
        let account_pubkeys = events
            .iter()
            .map(|event| {event.account_pubkey.clone()})
            .collect::<Vec<_>>();
        let owner_pubkeys = events
            .iter()
            .map(|event| {event.owner_pubkey.clone()})
            .collect::<Vec<_>>();
        let deltas: Vec<i64> = events
            .iter()
            .map(|event| {event.delta as i64})
            .collect::<Vec<_>>();
        sqlx::query!(
            r#"
            INSERT INTO events
                (slot, tx_sig, mint_pubkey, account_pubkey, owner_pubkey, delta)
            SELECT slot, tx_sig, mint_pubkey, account_pubkey, owner_pubkey, delta
            FROM UNNEST($1::bigint[], $2::varchar[], $3::varchar[], $4::varchar[], $5::varchar[], $6::bigint[])
                AS t(slot, tx_sig, mint_pubkey, account_pubkey, owner_pubkey, delta)
            ON CONFLICT (tx_sig, account_pubkey)
            DO UPDATE SET
                slot = EXCLUDED.slot,
                mint_pubkey = EXCLUDED.mint_pubkey,
                owner_pubkey = EXCLUDED.owner_pubkey,
                delta = EXCLUDED.delta
            "#,
            &slots,
            &tx_sigs,
            &mint_pubkeys,
            &account_pubkeys,
            &owner_pubkeys,
            &deltas,
        ).execute(&mut *tx).await?;
        tx.commit().await?;

        Ok(())
    }

    async fn confirm_events(&self, events: &[Event]) -> Result<(), Error> {
        todo!()
    }
}

