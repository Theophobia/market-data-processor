pub mod database;
pub mod trading_pair;
pub mod kline;
pub mod error;

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
		let first = PostgresAbsenceAnalyser::fetch_first_timeframe(pair).await;
		let last = PostgresAbsenceAnalyser::fetch_last_complete_timeframe(pair).await;

		if first.is_some() && last.is_some() {
			let first = first.unwrap();
			let last = last.unwrap();

			// println!("{}", first);
			// println!("{}", last);

			let _ = PostgresAbsenceAnalyser::fetch_all_in_timeframe(first, last, pair).await;
		}
	}
}
