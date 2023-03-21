use std::collections::{HashMap};
use std::time::SystemTime;
use sqlx::Row;
use crate::database::postgres::PostgresDatabase;
use crate::trading_pair::TradingPair;
use crate::logger::logger::{Logger, LogLevel};

pub struct PostgresAbsenceAnalyser {}

impl PostgresAbsenceAnalyser {
	pub async fn analyze(db: &PostgresDatabase, pairs: &Vec<TradingPair>) -> HashMap<TradingPair, Vec<u64>> {
		let mut map: HashMap<TradingPair, Vec<u64>> = HashMap::with_capacity(pairs.len());

		for pair in pairs {
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
	pub async fn get_first_timeframe(db: &PostgresDatabase, pair: &TradingPair) -> Option<u64> {
		Logger::log_str(
			LogLevel::FINE,
			"get_first_timeframe() database/analyzer.rs",
			format!("Entering function with trading_pair={pair}").as_str()
		);

		let pair_lower = pair.to_string().to_lowercase();

		let query = format!(r"
			SELECT MIN(time_open) FROM klines_{pair_lower};
		");

		let res = sqlx::query(query.as_str()).fetch_optional(&db.pool).await;
		if res.is_err() {
			Logger::log_str(
				LogLevel::INFO,
				"get_first_timeframe() database/analyzer.rs",
				"Query \"SELECT MIN(time_open)...\" returned error, function is returning None as a result"
			);
			return None;
		}

		let res = res.unwrap();
		if res.is_none() {
			Logger::log_str(
				LogLevel::FINE,
				"get_last_timeframe() database/analyzer.rs",
				format!("Query \"SELECT MIN(time_open)...\" returned Option as None (no MIN time_open exists, table is empty), function is returning None as a result").as_str()
			);
			return None;
		}

		let row = res.unwrap();
		let res: Result<i64, sqlx::Error> = row.try_get(0);
		if res.is_err() {
			Logger::log_str(
				LogLevel::FINE,
				"get_last_timeframe() database/analyzer.rs",
				format!("Could not get first element of row for some reason? Function is returning None as a result").as_str()
			);
			return None;
		}

		let time = res.unwrap() as u64;

		Logger::log_str(
			LogLevel::FINE,
			"get_last_timeframe() database/analyzer.rs",
			format!("Found first timeframe to be {time}, returning that as a result").as_str()
		);
		Some(time)
	}

	pub async fn get_last_timeframe(db: &PostgresDatabase, pair: &TradingPair) -> Option<u64> {
		Logger::log_str(
			LogLevel::FINE,
			"get_last_timeframe() database/analyzer.rs",
			format!("Entering function with trading_pair={pair}").as_str()
		);

		let pair_lower = pair.to_string().to_lowercase();

		let query = format!(r"
			SELECT MAX(time_open) FROM klines_{pair_lower};
		");

		let res = sqlx::query(query.as_str()).fetch_optional(&db.pool).await;
		if res.is_err() {
			Logger::log_str(
				LogLevel::INFO,
				"get_last_timeframe() database/analyzer.rs",
				"Query \"SELECT MAX(time_open)...\" returned error, function is returning None as a result"
			);
			return None;
		}

		let res = res.unwrap();
		if res.is_none() {
			Logger::log_str(
				LogLevel::FINE,
				"get_last_timeframe() database/analyzer.rs",
				format!("Query \"SELECT MAX(time_open)...\" returned Option as None (no MAX time_open exists, table is empty), function is returning None as a result").as_str()
			);
			return None;
		}

		let row = res.unwrap();
		let res: Result<i64, sqlx::Error> = row.try_get(0);
		if res.is_err() {
			Logger::log_str(
				LogLevel::FINE,
				"get_last_timeframe() database/analyzer.rs",
				format!("Could not get first element of row for some reason? Function is returning None as a result").as_str()
			);
			return None;
		}

		let time = res.unwrap() as u64;

		Logger::log_str(
			LogLevel::FINE,
			"get_last_timeframe() database/analyzer.rs",
			format!("Found last timeframe to be {time}, returning that as a result").as_str()
		);
		Some(time)
	}

	pub async fn get_last_timeframe_pot(db: &PostgresDatabase, pair: &TradingPair) -> Option<u64> {
		Logger::log_str(
			LogLevel::FINE,
			"get_last_timeframe_pot() database/analyzer.rs",
			format!("Entering function with trading_pair={pair}").as_str()
		);

		let pair_lower = pair.to_string().to_lowercase();

		let query = format!(r"
			SELECT MAX(time_open) FROM pot_{pair_lower};
		");

		let res = sqlx::query(query.as_str()).fetch_optional(&db.pool).await;
		if res.is_err() {
			Logger::log_str(
				LogLevel::INFO,
				"get_last_timeframe_pot() database/analyzer.rs",
				"Query \"SELECT MAX(time_open)...\" returned error, function is returning None as a result"
			);
			return None;
		}

		let res = res.unwrap();
		if res.is_none() {
			Logger::log_str(
				LogLevel::FINE,
				"get_last_timeframe_pot() database/analyzer.rs",
				format!("Query \"SELECT MAX(time_open)...\" returned Option as None (no MAX time_open exists, table is empty), function is returning None as a result").as_str()
			);
			return None;
		}

		let row = res.unwrap();
		let res: Result<i64, sqlx::Error> = row.try_get(0);
		if res.is_err() {
			Logger::log_str(
				LogLevel::FINE,
				"get_last_timeframe_pot() database/analyzer.rs",
				format!("Could not get first element of row for some reason? Function is returning None as a result").as_str()
			);
			return None;
		}

		let time = res.unwrap() as u64;

		Logger::log_str(
			LogLevel::FINE,
			"get_last_timeframe() database/analyzer.rs",
			format!("Found last timeframe to be {time}, returning that as a result").as_str()
		);
		Some(time)
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
