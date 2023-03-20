pub mod database;
pub mod trading_pair;
pub mod kline;
pub mod error;
pub mod api_connector;

use crate::api_connector::{BinanceConnector, Connector};
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresDatabaseSetup};
use crate::kline::Kline;

#[tokio::main]
async fn main() {
	use crate::trading_pair::TradingPair::*;

	dotenv::dotenv().unwrap();

	let pairs = vec![BTCUSDT];
	let db = PostgresDatabase::new().await;
	PostgresDatabaseSetup::setup(&db, &pairs).await;
	let missing = PostgresAbsenceAnalyser::analyze(&db, &pairs).await;

	for pair in pairs {
		let first = BinanceConnector::fetch_first_timeframe(pair).await;
		let last = BinanceConnector::fetch_last_complete_timeframe(pair).await;

		if first.is_some() && last.is_some() {
			let first = first.unwrap();
			let last = last.unwrap();

			// println!("{}", first);
			// println!("{}", last);

			let klines = BinanceConnector::fetch_all_in_timeframe(first, last, pair).await;
			if klines.is_err() {
				eprintln!("Error fetching {pair}");
				continue;
			}

			let klines: Vec<Kline> = klines.unwrap();

			println!("Klines: {klines:?}");
			println!("Klines length: {}", klines.len());
		}
	}
}
