use async_trait::async_trait;

use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Mutex;
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

#[async_trait]
pub trait Connector {
	async fn fetch_first_timeframe(trading_pair: TradingPair) -> Option<u64>;
	async fn fetch_last_complete_timeframe(trading_pair: TradingPair) -> Option<u64>;
	async fn fetch_all_in_timeframe(from: u64, to: u64, trading_pair: TradingPair) -> Result<Vec<Kline>, BusinessError>;
}

pub struct TimeframeJumpGenerator {}

impl TimeframeJumpGenerator {
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
}

pub struct BinanceConnector {}

#[async_trait]
impl Connector for BinanceConnector {
	async fn fetch_first_timeframe(trading_pair: TradingPair) -> Option<u64> {
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

	async fn fetch_last_complete_timeframe(trading_pair: TradingPair) -> Option<u64> {
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

	async fn fetch_all_in_timeframe(from: u64, to: u64, trading_pair: TradingPair) -> Result<Vec<Kline>, BusinessError> {

		// Checks for wrong arguments
		if from % 60_000 != 0 || to % 60_000 != 0 {
			return Err(BusinessError::UNALIGNED_TIMEFRAME);
		}

		if !(from <= to) {
			return Err(BusinessError::INCORRECT_ARGUMENT_ORDER);
		}

		// Get timeframe jumps
		let timeframe_jumps = TimeframeJumpGenerator::generate_timeframe_jumps(from, to).await;
		if timeframe_jumps.is_err() {
			return Err(BusinessError::UNSPECIFIED_ERROR);
		}
		let timeframe_jumps = timeframe_jumps.unwrap();
		// let timeframe_jumps: Vec<u64> = timeframe_jumps.iter().take(10).cloned().collect(); // TODO
		let timeframe_jumps_len = timeframe_jumps.len();

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

		let mut klines: Vec<Kline> = Vec::with_capacity(timeframe_jumps_len);
		let klines_rc = Mutex::new(klines);

		bodies.for_each(|body| async {
			match body {
				Some(t) => {
					t.iter()
						.filter(|&kline| kline.time_open >= from && kline.time_open <= to)
						.for_each(|kline| klines_rc.lock().unwrap().push(kline.clone()));
				}
				None => eprintln!("Got an error"),
			}
		}).await;

		Ok(klines_rc.into_inner().unwrap())
	}
}
