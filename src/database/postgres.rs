use async_trait::async_trait;
use std::env;
use sqlx::{Error, PgPool, Pool, Postgres, Transaction};
use sqlx::postgres::{PgQueryResult, PgRow};
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
	pub async fn setup(db: &PostgresDatabase, trading_pairs: &Vec<TradingPair>) -> Option<sqlx::Error> {
		for pair in trading_pairs {
			let res = Self::setup_tp(db, pair).await;
			if res.is_some() {
				return res;
			}
		}

		None
	}

	pub async fn setup_tp(db: &PostgresDatabase, trading_pair: &TradingPair) -> Option<sqlx::Error> {
		let pair_lower = trading_pair.to_string().to_lowercase();

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
	pub async fn insert_klines(db: &PostgresDatabase, trading_pair: TradingPair, klines: &Vec<Kline>) -> Option<BusinessError> {
		let pair_lower = trading_pair.to_string().to_lowercase();

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
					format!("Committed items").as_str()
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
}
