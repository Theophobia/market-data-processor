pub mod database;
pub mod trading_pair;
pub mod kline;

use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresDatabaseSetup};

#[tokio::main]
async fn main() {
	use crate::trading_pair::TradingPair::*;

	dotenv::dotenv().unwrap();

	let pairs = vec![BTCUSDT];
	let db = PostgresDatabase::new().await;
	PostgresDatabaseSetup::setup(&db, &pairs).await;
	let missing = PostgresAbsenceAnalyser::analyze(&db, &pairs).await;

	for pair in pairs {
		PostgresAbsenceAnalyser::fetch_first_timestamp(pair).await;
	}

	println!("{missing:?}");
}
