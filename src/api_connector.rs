use async_trait::async_trait;

use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use chrono::Utc;
use sqlx::Row;
use futures::{stream, StreamExt};
use reqwest::{Client, Error, Response, StatusCode};
use tokio;
use tokio::time::sleep;
use crate::cooldown_handler::CooldownHandler;
use crate::database::db::Database;
use crate::database::postgres::PostgresDatabase;
use crate::error::BusinessError;
use crate::trading_pair::TradingPair;
use crate::kline::*;
use crate::logger::logger::{Logger, LogLevel};

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

		let mut res = Vec::with_capacity(((to - from) / (1000 * 60_000) + 1) as usize); // TODO: Overflows on 32-bit machine?
		let mut curr_time = from;

		Logger::log_str(
			LogLevel::FINE,
			"generate_timeframe_jumps() api_connector.rs",
			format!("Timeframe jumps vector has an initial capacity of {}", res.capacity()).as_str()
		);

		while curr_time <= to {
			res.push(curr_time);
			curr_time += 1000 * 60_000;
		}

		Logger::log_str(
			LogLevel::FINE,
			"generate_timeframe_jumps() api_connector.rs",
			format!("Timeframe jumps vector has an end size of {}", res.len()).as_str()
		);

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
		let time = kline.time_open;

		Logger::log_str(
			LogLevel::FINE,
			"fetch_last_complete_timeframe() api_connector.rs",
			format!("First timeframe is found to be {time}").as_str()
		);

		Some(time)
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

		Logger::log_str(
			LogLevel::FINE,
			"fetch_last_complete_timeframe() api_connector.rs",
			format!("Last complete timeframe is found to be {min_time}").as_str()
		);

		Some(min_time)
	}

	async fn fetch_all_in_timeframe(from: u64, to: u64, trading_pair: TradingPair) -> Result<Vec<Kline>, BusinessError> {

		// Checks for wrong arguments
		if from % 60_000 != 0 || to % 60_000 != 0 {
			Logger::log_str(
				LogLevel::FINE,
				"fetch_all_in_timeframe() api_connector.rs",
				"Passed unaligned timeframes, returning error"
			);
			return Err(BusinessError::UNALIGNED_TIMEFRAME);
		}

		if !(from <= to) {
			Logger::log_str(
				LogLevel::FINE,
				"fetch_all_in_timeframe() api_connector.rs",
				"Arguments are switched up, returning error"
			);
			return Err(BusinessError::INCORRECT_ARGUMENT_ORDER);
		}

		// Get timeframe jumps
		let timeframe_jumps = TimeframeJumpGenerator::generate_timeframe_jumps(from, to).await;
		if timeframe_jumps.is_err() {
			Logger::log_str(
				LogLevel::FINE,
				"fetch_all_in_timeframe() api_connector.rs",
				"Timeframe jump generation threw an error, returning error"
			);
			return Err(BusinessError::UNSPECIFIED_ERROR);
		}
		let timeframe_jumps = timeframe_jumps.unwrap();
		// let timeframe_jumps: Vec<u64> = timeframe_jumps.iter().take(10).cloned().collect(); // TODO
		let timeframe_jumps_len = timeframe_jumps.len();

		// Specify thread count
		let thread_count = 10;
		Logger::log_str(
			LogLevel::FINE,
			"fetch_all_in_timeframe() api_connector.rs",
			format!("Thread count is set to {thread_count}").as_str()
		);

		// Create the shared queue
		let mut queue = VecDeque::with_capacity(timeframe_jumps.len());
		Logger::log_str(
			LogLevel::FINE,
			"fetch_all_in_timeframe() api_connector.rs",
			format!("Created queue with capacity {}", queue.capacity()).as_str()
		);

		// Enqueue the timeframes
		for jump in timeframe_jumps {
			queue.push_back(jump);
		}
		Logger::log_str(
			LogLevel::FINE,
			"fetch_all_in_timeframe() api_connector.rs",
			format!("Enqueued {} timeframes for requests", queue.len()).as_str()
		);

		let request_client = Client::new();

		// Run the requests
		Logger::log_str(
			LogLevel::FINE,
			"fetch_all_in_timeframe() api_connector.rs",
			"Spinning the request threads up"
		);
		let bodies = stream::iter(queue).map(|timeframe| {
			let client = &request_client;

			async move {
				if !CooldownHandler::can_request() {
					let now = Utc::now().timestamp_millis();
					let until = CooldownHandler::get_until().unwrap();

					Logger::log_str(
						LogLevel::FINE,
						"fetch_all_in_timeframe() api_connector.rs",
						format!("Sleeping because of cooldown, until={until}").as_str()
					);

					sleep(Duration::from_millis((until - now) as u64)).await;
				}

				let url = format!(r"
					https://data.binance.com/api/v3/klines?symbol={trading_pair}&startTime={timeframe}&interval=1m&limit=1000
				");
				let res: Result<Response, Error> = client.get(url.clone()).send().await;

				if res.is_err() {
					Logger::log_str(
						LogLevel::FINE,
						"fetch_all_in_timeframe() api_connector.rs",
						format!("Bad request to {url}").as_str()
					);
					return None;
				}

				let res: Response = res.unwrap();
				let status_code: StatusCode = res.status();
				let headers = res.headers().clone();

				let json = res.json::<Vec<KlinePreProcess>>().await;
				if json.is_err() {
					Logger::log_str(
						LogLevel::FINE,
						"fetch_all_in_timeframe() api_connector.rs",
						"Could not deserialize to Vec<KlinePreProcess>"
					);
					Logger::log_str(
						LogLevel::FINE,
						"fetch_all_in_timeframe() api_connector.rs",
						format!("Status code {status_code}").as_str()
					);

					match status_code {
						StatusCode::TOO_MANY_REQUESTS => {
							let now = Utc::now().timestamp_millis();
							let next_minute = now - now % 60_000 + 60_000;
							CooldownHandler::set_cooldown_to(next_minute);
						},
						StatusCode::IM_A_TEAPOT => {
							// let now = Utc::now().timestamp_millis();
							// let next_minute = now - now % 60_000 + 3 * 60_000;
							// CooldownHandler::set_cooldown_to(next_minute);

							let retry_after = headers.get("Retry-After");
							Logger::log_str(
								LogLevel::FINE,
								"fetch_all_in_timeframe() api_connector.rs",
								format!("Got teapotted (418)").as_str()
							);
							if retry_after.is_some() {
								let retry_after = i64::from_str(retry_after.unwrap().clone().to_str().unwrap()).unwrap();
								CooldownHandler::set_cooldown_to(retry_after);

								Logger::log_str(
									LogLevel::FINE,
									"fetch_all_in_timeframe() api_connector.rs",
									format!("retry_after={retry_after}").as_str()
								);
							}
						},
						_ => {}
					}

					return None;
				}

				let json: Vec<KlinePreProcess> = json.unwrap();
				let json: Vec<Kline> = json.iter().map(|k| k.process()).collect();

				Logger::log_str(
					LogLevel::FINE,
					"fetch_all_in_timeframe() api_connector.rs",
					format!("Fetched timestamp={timeframe} successfully, size is {} after filtering", json.len()).as_str()
				);

				Some(json)
			}
		}).buffer_unordered(thread_count);

		let klines: Vec<Kline> = Vec::with_capacity(1000 * timeframe_jumps_len);
		Logger::log_str(
			LogLevel::FINE,
			"fetch_all_in_timeframe() api_connector.rs",
			format!("Klines vector initial capacity is {}", klines.capacity()).as_str()
		);
		let klines_mutex = Mutex::new(klines);

		bodies.for_each(|body| async {
			match body {
				Some(t) => {
					t.iter()
						.filter(|&kline| kline.time_open >= from && kline.time_open <= to)
						.for_each(|kline| klines_mutex.lock().unwrap().push(kline.clone()));
				},
				None => {
					Logger::log_str(
						LogLevel::FINE,
						"fetch_all_in_timeframe() api_connector.rs",
						"Encountered empty response in bodies"
					);
					eprintln!("Got an error")
				},
			}
		}).await;

		Logger::log_str(
			LogLevel::FINE,
			"fetch_all_in_timeframe() api_connector.rs",
			format!("End of function, klines vector has a size of {}", klines_mutex.lock().unwrap().len()).as_str()
		);

		Ok(klines_mutex.into_inner().unwrap())
	}
}
