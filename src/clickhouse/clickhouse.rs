use anyhow::Error;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct Event {
    pub slot: u64,
    pub tx_sig: String,
    pub mint_pubkey: String,
    pub account_pubkey: String,
    pub owner_pubkey: String,
    pub delta: Decimal,
    pub confirmed: u8,
    pub _timestamp: DateTime<Utc>,
}

pub struct ClickHouse {
    pub client: Client
}

impl ClickHouse {
    fn new(url: &str, user: &str, password: &str) -> Self {
        let client = Client::default()
            .with_password(password)
            .with_user(user)
            .with_url(url);
        Self {
            client
        }
    }

    fn confirm_events(&self, events: &[Event]) -> Result<(), Error> {
        if events.is_empty() {
            return Ok(());
        }

        Ok(())
    }
}