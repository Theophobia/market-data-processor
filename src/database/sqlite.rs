use async_trait::async_trait;
use std::env;
use std::path::Path;
use std::process::Command;
use sqlx::{Pool, Sqlite, SqlitePool};
use sqlx::sqlite::{SqliteQueryResult, SqliteRow};
use crate::database::db::Database;

pub struct SqliteDatabase {
	pool: Pool<Sqlite>,
}

impl SqliteDatabase {}

#[async_trait]
impl Database<SqliteQueryResult, SqliteRow> for SqliteDatabase {
	async fn new() -> Self {
		let url: &String = &env::var("DATABASE_SQLITE_URL").unwrap();
		if url.starts_with("sqlite:") {
			let db_file_name: &str = url.split_once(":").unwrap().1;
			if !Path::new(db_file_name).exists() {
				// let db_file: File = File::create(db_file_name).unwrap();
				Command::new("sqlite3").arg(db_file_name).arg("CREATE TABLE users (name TEXT); DROP TABLE users;").output().unwrap();
			}
		}

		Self {
			pool: SqlitePool::connect(url).await.unwrap()
		}
	}

	async fn execute(&self, query: &str) -> Result<SqliteQueryResult, sqlx::Error> {
		sqlx::query(query).execute(&self.pool).await
	}

	async fn fetch_optional(&self, query: &str) -> Option<SqliteRow> {
		let res = sqlx::query(query).fetch_optional(&self.pool).await;

		if res.is_ok() {
			res.unwrap()
		}
		else {
			None
		}
	}
}

pub struct SqliteDatabaseSetup {}

impl SqliteDatabaseSetup {
	pub async fn setup(db: &SqliteDatabase) -> Option<sqlx::Error> {
		let create_table_string = r"
		create table if not exists klines
		(
			time_open  INTEGER not null
				constraint table_name_pk
					primary key,
			open       REAL    not null,
			high       REAL    not null,
			low        REAL    not null,
			close      REAL    not null,
			volume     REAL    not null,
			num_trades INTEGER not null
		);

		create unique index klines_time_open_uindex
			on klines (time_open);
	 	";
		let res = db.execute(create_table_string).await;

		if res.is_err() {
			return res.err();
		}

		None
	}
}

