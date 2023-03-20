use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;
use sqlx::Row;
use futures::{stream, StreamExt};
use reqwest::{Client, Error, Response};
use tokio;
use crate::database::db::Database;
use crate::database::postgres::PostgresDatabase;
use crate::error::BusinessError;
use crate::trading_pair::TradingPair;
use crate::kline::*;


pub struct PostgresAbsenceAnalyser {}

impl PostgresAbsenceAnalyser {
	pub async fn analyze(db: &PostgresDatabase, trading_pairs: &Vec<TradingPair>) -> HashMap<TradingPair, Vec<u64>> {
		let mut map: HashMap<TradingPair, Vec<u64>> = HashMap::with_capacity(trading_pairs.len());

		for pair in trading_pairs {
			let pair_lower = pair.to_string().to_lowercase();

			let query = format!(r"
				SELECT pot_{pair_lower}.time_open
				FROM pot_{pair_lower}
				LEFT JOIN klines_{pair_lower} ON pot_{pair_lower}.time_open = klines_{pair_lower}.time_open
				WHERE klines_{pair_lower}.time_open IS NULL;
			");

			let missing: Vec<u64> = sqlx::query(query.as_str()).fetch_all(&db.pool).await.unwrap()
				.iter()
				.map(|row| {
					let a: i64 = row.get(0);
					a as u64
				})
				.collect();

			map.insert(pair.clone(), missing);
		}

		map
	}

	/***
		If this returns None, should fetch first timestamp from Binance API
		Or some SQL error occurred
	 */
	pub async fn get_first_timeframe(db: &PostgresDatabase, trading_pair: TradingPair) -> Option<u64> {
		let pair_lower = trading_pair.to_string().to_lowercase();

		let query = format!(r"
			SELECT MIN(time_open) FROM klines_{pair_lower};
		");

		let res = sqlx::query(query.as_str()).fetch_optional(&db.pool).await;
		if res.is_err() {
			return None;
		}

		let res = res.unwrap();
		if res.is_none() {
			return None;
		}

		let row = res.unwrap();
		let res: Result<i64, sqlx::Error> = row.try_get(0);
		if res.is_err() {
			return None;
		}

		let time = res.unwrap() as u64;

		Some(time)
	}

	pub async fn get_last_timeframe(db: &PostgresDatabase, trading_pair: TradingPair) -> Option<u64> {
		let pair_lower = trading_pair.to_string().to_lowercase();

		let query = format!(r"
			SELECT MAX(time_open) FROM klines_{pair_lower};
		");

		let res = sqlx::query(query.as_str()).fetch_optional(&db.pool).await;
		if res.is_err() {
			return None;
		}

		let res = res.unwrap();
		if res.is_none() {
			return None;
		}

		let row = res.unwrap();
		let res: Result<i64, sqlx::Error> = row.try_get(0);
		if res.is_err() {
			return None;
		}

		let time = res.unwrap() as u64;

		Some(time)
	}

	pub async fn fetch_first_timeframe(trading_pair: TradingPair) -> Option<u64> {
		let url = format!(r"
			https://data.binance.com/api/v3/klines?symbol={trading_pair}&interval=1m&startTime=0&limit=1
		");

		let res = reqwest::get(&url).await;
		if res.is_err() {
			return None;
		}

		let klines = res.unwrap().json::<Vec<KlinePreProcess>>().await;
		if klines.is_err() {
			return None;
		}

		let klines = klines.unwrap();
		if klines.len() != 1 {
			return None;
		}

		let kline = klines.get(0).unwrap().process();

		Some(kline.time_open)
	}

	pub async fn fetch_last_complete_timeframe(trading_pair: TradingPair) -> Option<u64> {
		let url = format!("https://data.binance.com/api/v3/klines?symbol={trading_pair}&interval=1m&limit=2");

		let res = reqwest::get(&url).await;
		if res.is_err() {
			return None;
		}

		let klines = res.unwrap().json::<Vec<KlinePreProcess>>().await;
		if klines.is_err() {
			return None;
		}

		let klines = klines.unwrap();
		if klines.len() != 2 {
			return None;
		}

		let kline_left = klines.get(0).unwrap().process();
		let kline_right = klines.get(1).unwrap().process();

		// Binance API sanity check
		let min_time = {
			if kline_left.time_open < kline_right.time_open {
				kline_left.time_open
			} else {
				kline_right.time_open
			}
		};

		Some(min_time)
	}

	pub async fn fetch_all_in_timeframe(from: u64, to: u64, trading_pair: TradingPair) -> Result<(), BusinessError> {

		// Checks for wrong arguments
		if from % 60_000 != 0 || to % 60_000 != 0 {
			return Err(BusinessError::UNALIGNED_TIMEFRAME);
		}

		if !(from <= to) {
			return Err(BusinessError::INCORRECT_ARGUMENT_ORDER);
		}

		// Get timeframe jumps
		let timeframe_jumps = Self::generate_timeframe_jumps(from, to).await;
		if timeframe_jumps.is_err() {
			return Err(BusinessError::UNSPECIFIED_ERROR);
		}
		let timeframe_jumps = timeframe_jumps.unwrap();
		let timeframe_jumps: Vec<u64> = timeframe_jumps.iter().take(10).cloned().collect(); // TODO

		// Specify thread count
		let thread_count = 6;

		// Create the shared queue
		let mut queue = VecDeque::with_capacity(timeframe_jumps.len());

		// Enqueue the timeframes
		for jump in timeframe_jumps {
			queue.push_back(jump);
		}

		let request_client = Client::new();

		// Run the requests
		let bodies = stream::iter(queue).map(|timeframe| {
			let client = &request_client;

			async move {
				let res: Result<Response, Error> = client.get(format!(r"
					https://data.binance.com/api/v3/klines?symbol={trading_pair}&startTime={timeframe}&interval=1m&limit=1000
				")).send().await;

				if res.is_err() {
					return None;
				}

				let json: Vec<KlinePreProcess> = res.unwrap().json().await.unwrap();
				let json: Vec<Kline> = json.iter().map(|k| k.process()).collect();

				Some(json)
			}
		}).buffer_unordered(thread_count);

		bodies.for_each(|body| async {
			match body {
				Some(t) => println!("{t:?}"),
				None => eprintln!("Got an error"),
			}
		})
		.await;

		Ok(())
	}

	pub async fn generate_timeframe_jumps(from: u64, to: u64) -> Result<Vec<u64>, BusinessError> {
		if from % 60_000 != 0 || to % 60_000 != 0 {
			return Err(BusinessError::UNALIGNED_TIMEFRAME);
		}

		if !(from <= to) {
			return Err(BusinessError::INCORRECT_ARGUMENT_ORDER);
		}

		let mut res = Vec::with_capacity(((to - from) / (1000 * 60_000)) as usize); // TODO: Overflows on 32-bit machine?
		let mut curr_time = from;

		while curr_time <= to {
			res.push(curr_time);
			curr_time += 1000 * 60_000;
		}

		Ok(res)
	}

	pub fn curr_time_millis() -> u64 {
		SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("Time went backwards")
			.as_millis() as u64
	}

	pub fn last_complete_1m_timestamp() -> u64 {
		(Self::curr_time_millis() / (1 * 60_000) - 1) * (1 * 60_000)
	}

	pub fn last_complete_3m_timestamp() -> u64 {
		(Self::curr_time_millis() / (3 * 60_000) - 1) * (3 * 60_000)
	}

	pub fn last_complete_5m_timestamp() -> u64 {
		(Self::curr_time_millis() / (5 * 60_000) - 1) * (5 * 60_000)
	}

	pub fn last_complete_15m_timestamp() -> u64 {
		(Self::curr_time_millis() / (15 * 60_000) - 1) * (15 * 60_000)
	}
}
