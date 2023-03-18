use std::time::SystemTime;
use crate::database::postgres::PostgresDatabase;

pub struct PostgresAbsenceAnalyser {}

impl PostgresAbsenceAnalyser {
	pub async fn analyze(db: &PostgresDatabase) {
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
