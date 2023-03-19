use std::collections::HashMap;
use std::time::SystemTime;
use sqlx::Row;
use crate::database::postgres::PostgresDatabase;
use crate::trading_pair::TradingPair;

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
