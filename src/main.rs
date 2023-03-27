pub mod database;
pub mod trading_pair;
pub mod kline;
pub mod error;
pub mod api_connector;
pub mod logger;
pub mod cooldown_handler;
pub mod routine;
pub mod neural;

use crate::api_connector::BinanceConnector;
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresExecutor, PostgresSetup};

#[tokio::main]
async fn main() {
	use crate::trading_pair::TradingPair::*;

	dotenv::dotenv().unwrap();

	let pairs = vec![BTCUSDT, LDOUSDT, ETHUSDT];
	let db = PostgresDatabase::new().await;
	let api_con = BinanceConnector::new();

	PostgresSetup::setup(&db, &pairs).await;

	for pair in pairs.iter() {
		let _ = PostgresExecutor::fetch_insert_leading_trailing(&db, &api_con, pair).await;
		let _ = PostgresExecutor::update_possible_open_times(&db, &api_con, pair).await;
	}

	let _missing = PostgresAbsenceAnalyser::analyze(&db, &pairs).await;
}
