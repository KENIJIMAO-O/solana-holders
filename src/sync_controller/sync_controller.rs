use std::sync::Arc;
use solana_sdk::pubkey::Pubkey;
use sqlx::Database;
use sqlx::postgres::PgPoolOptions;
use crate::database::postgresql::DatabaseConnection;
use crate::message_queue::message_queue::Redis;
use crate::monitor::monitor::TokenEvent;
use crate::repositories::events::{Event, EventsRepository};

pub struct SyncController {
    pub redis: Arc<Redis>,
    pub database: Arc<DatabaseConnection>,
}

impl SyncController {
    pub fn new(
        redis: Arc<Redis>,
        database: Arc<DatabaseConnection>,
    ) -> Self {
        Self {
            redis,
            database
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 50;
        const BATCH_TIMEOUT_MS: usize = 100;
        let consumer_name = "sync_dequeuer";

        let mut datas = self.redis.batch_dequeue_holder_event(consumer_name, BATCH_SIZE, BATCH_TIMEOUT_MS).await?;
        // todo:拿到数据之后，下一步是将数据整合到数据库中
        let token_events: Vec<Event> = datas.into_iter().map(|data| {
            let event = match data.1.owner_address {
                None => {
                    Event::new(
                        data.1.slot,
                        data.1.tx_signature,
                        data.1.mint_address.to_string(),
                        data.1.account_address.to_string(),
                        "".to_string(),
                        data.1.delta.parse().unwrap(),
                        data.1.confirmed
                    )
                }
                Some(owner_pubkey) => {
                    Event::new(
                        data.1.slot,
                        data.1.tx_signature,
                        data.1.mint_address.to_string(),
                        data.1.account_address.to_string(),
                        owner_pubkey.to_string(),
                        data.1.delta.parse().unwrap(),
                        data.1.confirmed
                    )
                }
            };
            event
        }).collect();

        self.database.upsert_events_btach(&token_events).await.unwrap();
        Ok(())
    }
}