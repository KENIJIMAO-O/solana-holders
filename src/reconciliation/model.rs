use crate::baseline::getProgramAccounts::HttpClient;
use crate::database::postgresql::DatabaseConnection;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize, Clone)]
pub struct SchedulingTier {
    pub threshold_percent: f64,
    pub interval_hours: i32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuditConfig {
    pub enabled: bool,
    pub tracked_mints: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub default_interval_hours: i32,
    pub scheduling_tiers: Vec<SchedulingTier>,
    pub audit: Option<AuditConfig>,
}

#[derive(Debug, Clone)]
pub struct ReconciliationServer {
    pub(crate) app_config: AppConfig,
    pub database: Arc<DatabaseConnection>,
    pub http_client: Arc<HttpClient>,
}

mod test {
    use super::*;
    #[test]
    fn test_load_config() {
        let settings = config::Config::builder()
            .add_source(config::File::with_name("config/default"))
            // 也可以从环境变量覆盖
            .add_source(config::Environment::with_prefix("APP"))
            .build()
            .unwrap();

        let res = settings.try_deserialize::<AppConfig>().unwrap();
        println!("res: {:#?}", res);
    }
}
