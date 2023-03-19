pub mod database;
pub mod trading_pair;

use serde::{Deserialize, Serialize};
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresDatabaseSetup};

#[allow(dead_code)]
#[derive(Deserialize)]
struct KlinePreProcess {
	time_open: u64,
	open: String,
	high: String,
	low: String,
	close: String,
	volume: String,
	time_close: u64,
	quote_asset_volume: String,
	num_trades: u64,
	taker_buy_base_asset_volume: String,
	taker_buy_base_quote_volume: String,
	unused: String,
}

#[allow(dead_code)]
impl KlinePreProcess {
	pub fn process(&self) -> Kline {
		let time_open = self.time_open;
		let open = self.open.parse::<f32>().unwrap();
		let high = self.high.parse::<f32>().unwrap();
		let low = self.low.parse::<f32>().unwrap();
		let close = self.close.parse::<f32>().unwrap();
		let volume = self.volume.parse::<f32>().unwrap();
		let num_trades = self.num_trades;

		Kline::new(time_open, open, high, low, close, volume, num_trades)
	}
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
struct Kline {
	time_open: u64,
	open: f32,
	high: f32,
	low: f32,
	close: f32,
	volume: f32,
	num_trades: u64,
}

#[allow(dead_code)]
impl Kline {
	pub fn new(time_open: u64, open: f32, high: f32, low: f32, close: f32, volume: f32, num_trades: u64) -> Self {
		Self { time_open, open, high, low, close, volume, num_trades }
	}
}


#[tokio::main]
async fn main() {
	use crate::trading_pair::TradingPair::*;

	dotenv::dotenv().unwrap();

	let pairs = vec![BTCUSDT];
	let db = PostgresDatabase::new().await;
	PostgresDatabaseSetup::setup(&db, &pairs).await;
	let missing = PostgresAbsenceAnalyser::analyze(&db, &pairs).await;

	println!("{missing:?}");
}
