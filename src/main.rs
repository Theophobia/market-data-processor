pub mod database;
pub mod trading_pair;
pub mod kline;
pub mod error;
pub mod api_connector;
pub mod logger;
pub mod cooldown_handler;

use std::process::exit;
use chrono::Utc;
use crate::api_connector::{BinanceConnector, Connector};
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresExecutor, PostgresSetup};
use crate::kline::Kline;
use crate::logger::logger::{Logger, LogLevel};

#[tokio::main]
async fn main() {
	use crate::trading_pair::TradingPair::*;

	dotenv::dotenv().unwrap();

	let pairs = vec![LDOUSDT];
	let db = PostgresDatabase::new().await;
	PostgresSetup::setup(&db, &pairs).await;
	let missing = PostgresAbsenceAnalyser::analyze(&db, &pairs).await;

	for pair in pairs {
		let first_remote = BinanceConnector::fetch_first_timeframe(pair).await;
		let last_remote = BinanceConnector::fetch_last_complete_timeframe(pair).await;

		let first_local = PostgresAbsenceAnalyser::get_first_timeframe(&db, pair).await;
		let last_local = PostgresAbsenceAnalyser::get_last_timeframe(&db, pair).await;

		println!("{first_local:?}");
		println!("{last_local:?}");

		if first_local.is_none() && last_local.is_none() {
			// Full download from remote

			if first_remote.is_some() && last_remote.is_some() {
				let first = first_remote.unwrap();
				let last = last_remote.unwrap();

				// println!("{}", first);
				// println!("{}", last);

				let klines = BinanceConnector::fetch_all_in_timeframe(first, last, pair).await;
				if klines.is_err() {
					eprintln!("Error fetching {pair}");
					continue;
				}

				let klines: Vec<Kline> = klines.unwrap();

				Logger::log_str(
					LogLevel::INFO,
					"main.rs",
					format!("Fetched klines, inserting now").as_str()
				);

				let res = PostgresExecutor::insert_klines(&db, pair, &klines).await;

				Logger::log_str(
					LogLevel::INFO,
					"main.rs",
					format!("Finished inserting klines").as_str()
				);

				// println!("Klines: {klines:?}");
				println!("Klines length: {}", klines.len());
			}
			else {
				exit(-123);
			}
		}

	}
}
