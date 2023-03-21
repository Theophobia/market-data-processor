use async_trait::async_trait;
use std::env;
use sqlx::{Error, PgPool, Pool, Postgres, Transaction};
use sqlx::postgres::{PgQueryResult, PgRow};
use crate::api_connector::Connector;
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::error::BusinessError;
use crate::kline::Kline;
use crate::logger::logger::{Logger, LogLevel};
use crate::trading_pair::TradingPair;

pub struct PostgresDatabase {
	pub(crate) pool: Pool<Postgres>,
}

impl PostgresDatabase {}

#[async_trait]
impl Database<PgQueryResult, PgRow> for PostgresDatabase {
	async fn new() -> Self {
		let url: &String = &env::var("DATABASE_POSTGRES_URL").unwrap();

		Self {
			pool: PgPool::connect(url).await.unwrap()
		}
	}

	async fn execute(&self, query: &str) -> Result<PgQueryResult, sqlx::Error> {
		sqlx::query(query).execute(&self.pool).await
	}

	async fn fetch_optional(&self, query: &str) -> Option<PgRow> {
		let res = sqlx::query(query).fetch_optional(&self.pool).await;

		if res.is_ok() {
			res.unwrap()
		} else {
			None
		}
	}
}

pub struct PostgresSetup {}

impl PostgresSetup {
	pub async fn setup(db: &PostgresDatabase, pairs: &Vec<TradingPair>) -> Option<sqlx::Error> {
		for pair in pairs {
			let res = Self::setup_tp(db, pair).await;
			if res.is_some() {
				return res;
			}
		}

		None
	}

	async fn setup_tp(db: &PostgresDatabase, pairs: &TradingPair) -> Option<sqlx::Error> {
		let pair_lower = pairs.to_string().to_lowercase();

		// Try to get a transaction from pool
		let tx: Result<Transaction<Postgres>, Error> = db.pool.begin().await;
		if tx.is_err() {
			return tx.err();
		}
		let mut tx = tx.unwrap();

		//
		// Create klines table
		//
		let query = format!(r"
		create table if not exists klines_{pair_lower}
		(
			time_open  bigint				not null
				constraint klines_{pair_lower}_pk
					primary key,
			open       double precision		not null,
			high       double precision		not null,
			low        double precision		not null,
			close      double precision 	not null,
			volume     double precision		not null,
			num_trades integer              not null
		);
		");
		sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();

		// Transfer ownership
		let query = format!(r"
		alter table klines_{pair_lower}
			owner to postgres;
		");
		sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();

		// Add index
		let query = format!(r"
		create unique index if not exists klines_{pair_lower}_time_open_uindex
			on klines_{pair_lower} (time_open);
		");
		sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();


		//
		// Create possible open times table (pot)
		//
		let query = format!(r"
		create table if not exists pot_{pair_lower}
		(
			time_open bigint not null
				constraint pot_{pair_lower}_pk
					primary key
		);
		");
		sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();

		// Transfer ownership
		let query = format!(r"
		alter table pot_{pair_lower}
			owner to postgres;
		");
		sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();

		// Add index
		let query = format!(r"
		create unique index if not exists pot_{pair_lower}_time_open_uindex
			on pot_{pair_lower} (time_open);
		");
		sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();


		// Commit all table generations
		let res = tx.commit().await;

		res.err()
	}
}

pub struct PostgresExecutor {}

impl PostgresExecutor {
	async fn insert_klines(db: &PostgresDatabase, pair: &TradingPair, klines: &Vec<Kline>) -> Option<BusinessError> {
		let pair_lower = pair.to_string().to_lowercase();

		// Try to get a transaction from pool
		let tx: Result<Transaction<Postgres>, Error> = db.pool.begin().await;
		if tx.is_err() {
			return Some(BusinessError::CANNOT_CREATE_SQL_TRANSACTION);
		}
		let mut tx = tx.unwrap();
		let mut i = 0;

		// Iterate over klines, insert those that are absent
		for kline in klines {
			if i == 10_000 {
				tx.commit().await.unwrap();
				tx = db.pool.begin().await.unwrap();
				i = 0;

				Logger::log_str(
					LogLevel::INFO,
					"insert_klines() postgres.rs",
					format!("Committed klines").as_str()
				);

			}
			i += 1;

			// let query = format!(r"
			// 	INSERT INTO klines_{pair_lower} (time_open, open, high, low, close, volume, num_trades)
			// 	VALUES {}, {}, {}, {}, {}, {}, {};
			// ", kline.time_open, kline.open, kline.high, kline.low, kline.close, kline.volume, kline.num_trades);

			let query = format!(r"
				INSERT INTO klines_{pair_lower} (time_open, open, high, low, close, volume, num_trades)
				SELECT {}, {}, {}, {}, {}, {}, {}
				WHERE NOT EXISTS (
					SELECT 1 FROM klines_{pair_lower} WHERE time_open = {}
				);
			", kline.time_open, kline.open, kline.high, kline.low, kline.close, kline.volume, kline.num_trades, kline.time_open);

			sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();
		}

		let res = tx.commit().await;

		if res.is_err() {
			return Some(BusinessError::SQL_TRANSACTION_ERROR);
		}

		None
	}

	async fn fetch_and_insert(db: &PostgresDatabase, connector: &impl Connector, pair: &TradingPair, first: u64, last: u64) {
		if first == last {
			Logger::log_str(
				LogLevel::INFO,
				"fetch_and_insert() postgres.rs",
				format!("First and last are equal, no klines to be fetched").as_str(),
			);
			return;
		}

		Logger::log_str(
			LogLevel::INFO,
			"fetch_and_insert() postgres.rs",
			format!("first={first}, last={last}").as_str(),
		);

		let klines = connector.fetch_all_in_timeframe(first, last, pair).await;
		if klines.is_err() {
			eprintln!("Error fetching {pair}");
			return;
		}

		let klines: Vec<Kline> = klines.unwrap();

		Logger::log_str(
			LogLevel::INFO,
			"fetch_and_insert() postgres.rs",
			format!("Fetched {} klines for pair {pair}, inserting now", klines.len()).as_str(),
		);

		let _ = PostgresExecutor::insert_klines(&db, pair, &klines).await;

		Logger::log_str(
			LogLevel::INFO,
			"fetch_and_insert() postgres.rs",
			format!("Finished inserting klines").as_str(),
		);

		// println!("Klines: {klines:?}");
		// println!("Klines length: {}", klines.len());
	}

	pub async fn fetch_insert_leading_trailing(db: &PostgresDatabase, connector: &impl Connector, pair: &TradingPair) {
		let first_remote = connector.fetch_first_timeframe(pair).await;
		let last_remote = connector.fetch_last_complete_timeframe(pair).await;

		let first_local = PostgresAbsenceAnalyser::get_first_timeframe(&db, pair).await;
		let last_local = PostgresAbsenceAnalyser::get_last_timeframe(&db, pair).await;

		// println!("first_remote = {first_remote:?}");
		// println!("last_remote = {last_remote:?}");
		// println!("first_local = {first_local:?}");
		// println!("last_local = {last_local:?}");

		if first_local.is_none() && last_local.is_none() {
			// Full download from remote

			if first_remote.is_some() && last_remote.is_some() {
				let first = first_remote.unwrap();
				let last = last_remote.unwrap();

				Self::fetch_and_insert(&db, connector, pair, first, last).await;
			} else {
				Logger::log_str(
					LogLevel::ERROR,
					"fetch_insert_leading_trailing() postgres.rs",
					"Logical error #1"
				);
			}
		} else if first_local.is_some() && last_local.is_some() {
			// Fetch from first remote to first local
			// Fetch from last local to last remote

			let first_remote = first_remote.unwrap();
			let last_remote = last_remote.unwrap();

			let first_local = first_local.unwrap();
			let last_local = last_local.unwrap();

			Self::fetch_and_insert(&db, connector, pair, first_remote, first_local).await;
			Self::fetch_and_insert(&db, connector, pair, last_local, last_remote).await;
		}
	}

	pub async fn insert_possible_open_times(db: &PostgresDatabase, pair: &TradingPair, times: &Vec<u64>) -> Option<BusinessError> {
		let pair_lower = pair.to_string().to_lowercase();

		// Try to get a transaction from pool
		let tx: Result<Transaction<Postgres>, Error> = db.pool.begin().await;
		if tx.is_err() {
			return Some(BusinessError::CANNOT_CREATE_SQL_TRANSACTION);
		}
		let mut tx = tx.unwrap();
		let mut i = 0;

		// Iterate over klines, insert those that are absent
		for time in times.iter() {
			if i == 10_000 {
				tx.commit().await.unwrap();
				tx = db.pool.begin().await.unwrap();
				i = 0;

				Logger::log_str(
					LogLevel::INFO,
					"insert_possible_open_times() postgres.rs",
					format!("Committed possible open times for pair {pair}").as_str()
				);
			}
			i += 1;

			let query = format!(r"
				INSERT INTO pot_{pair_lower} (time_open)
				SELECT {time}
				WHERE NOT EXISTS (
					SELECT 1 FROM pot_{pair_lower} WHERE time_open = {time}
				);
			");

			sqlx::query(query.as_str()).execute(&mut tx).await.unwrap();
		}

		let res = tx.commit().await;
		Logger::log_str(
			LogLevel::INFO,
			"insert_possible_open_times() postgres.rs",
			format!("Committed possible open times for pair {pair} (outside loop here)").as_str()
		);

		if res.is_err() {
			return Some(BusinessError::SQL_TRANSACTION_ERROR);
		}

		None
	}

	pub async fn update_possible_open_times(db: &PostgresDatabase, connector: &impl Connector, pair: &TradingPair) -> Option<BusinessError>{
		let first_remote = connector.fetch_first_timeframe(pair).await;
		let last_remote = connector.fetch_last_complete_timeframe(pair).await;

		// let first_local_pot = PostgresAbsenceAnalyser::get_first_timeframe_pot(&db, pair).await;
		let last_local_pot = PostgresAbsenceAnalyser::get_last_timeframe_pot(&db, pair).await;

		if first_remote.is_none() {
			Logger::log_str(
				LogLevel::ERROR,
				"update_possible_open_times() postgres.rs",
				"Could not fetch first_remote"
			);

			return Some(BusinessError::API_CONNECTOR_ERROR);
		}

		if last_remote.is_none() {
			Logger::log_str(
				LogLevel::ERROR,
				"update_possible_open_times() postgres.rs",
				"Could not fetch last_remote"
			);

			return Some(BusinessError::API_CONNECTOR_ERROR);
		}

		let first_remote = first_remote.unwrap();
		let last_remote = last_remote.unwrap();

		let start_time = {
			if last_local_pot.is_none() {
				first_remote
			}
			else {
				last_local_pot.unwrap()
			}
		};
		let mut curr_time = start_time;
		let end_time = last_remote;

		let mut times: Vec<u64> = Vec::new(); // TODO add with capacity

		while curr_time <= end_time {
			times.push(curr_time);
			curr_time += 60_000;
		}

		let res = Self::insert_possible_open_times(&db, pair, &times).await;

		if res.is_some() {
			Logger::log_str(
				LogLevel::ERROR,
				"update_possible_open_times() postgres.rs",
				"Error inserting possible open times"
			);

			return Some(BusinessError::UNSPECIFIED_ERROR);
		}

		None
	}
}

