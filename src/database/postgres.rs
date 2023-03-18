use async_trait::async_trait;
use std::env;
use sqlx::{Error, PgPool, Pool, Postgres, Transaction};
use sqlx::postgres::{PgQueryResult, PgRow};
use crate::database::db::Database;

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

pub struct PostgresDatabaseSetup {}

impl PostgresDatabaseSetup {
	pub async fn setup(db: &PostgresDatabase) -> Option<sqlx::Error> {

		// Try to get a transaction from pool
		let tx: Result<Transaction<Postgres>, Error> = db.pool.begin().await;
		if tx.is_err() {
			return tx.err();
		}
		let mut tx = tx.unwrap();

		//
		// Create klines table
		//
		sqlx::query(r"
		create table if not exists klines
		(
			time_open  bigint				not null
				constraint klines_pk
					primary key,
			open       double precision		not null,
			high       double precision		not null,
			low        double precision		not null,
			close      double precision 	not null,
			volume     double precision		not null,
			num_trades integer              not null
		);
		").execute(&mut tx).await.unwrap();

		// Add index and transfer ownership
		sqlx::query(r"
		alter table klines
			owner to postgres;
		").execute(&mut tx).await.unwrap();
		sqlx::query(r"
		create unique index if not exists klines_time_open_uindex
			on klines (time_open);
		").execute(&mut tx).await.unwrap();

		//
		// Create possible open times table
		//
		sqlx::query(r"
		create table if not exists possible_open_times
		(
			time_open bigint not null
				constraint possible_open_times_pk
					primary key
		);
		").execute(&mut tx).await.unwrap();

		// Add index and transfer ownership
		sqlx::query(r"
		alter table possible_open_times
			owner to postgres;
		").execute(&mut tx).await.unwrap();
		sqlx::query(r"
		create unique index if not exists possible_open_times_time_open_uindex
			on possible_open_times (time_open);
		").execute(&mut tx).await.unwrap();

		let res = tx.commit().await;

		// // Try to get a transaction from pool
		// let mut tx: Transaction<Postgres>;
		// tx = db.pool.begin().await.unwrap();
		//
		// let mut i = 0;
		// let start_time = 1568887320000u64;
		// let mut curr_time = start_time;
		// let end_time = PostgresAbsenceAnalyser::last_complete_1m_timestamp();
		//
		// // 1000 inserts at a time, insert all
		// while curr_time <= end_time {
		// 	if i == 1000 {
		// 		let _res = tx.commit().await;
		// 		tx = db.pool.begin().await.unwrap();
		// 		i = 0;
		// 	} else { i += 1; }
		//
		// 	sqlx::query(format!("insert into possible_open_times (time_open) select ({curr_time}) where not exists (select 1 from possible_open_times where time_open = {curr_time})").as_str())
		// 		.execute(&mut tx).await.unwrap();
		// 	curr_time += 60_000;
		// }
		//
		// let res = tx.commit().await;

		res.err()
	}
}
