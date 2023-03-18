use async_trait::async_trait;
use std::env;
use std::time::Duration;
use sqlx::{MySql, MySqlPool, Pool};
use sqlx::mysql::{MySqlQueryResult, MySqlRow};
use crate::database::db::Database;

pub struct MySqlDatabase {
	pool: Pool<MySql>,
}

impl MySqlDatabase {}

#[async_trait]
impl Database<MySqlQueryResult, MySqlRow> for MySqlDatabase {
	async fn new() -> Self {
		let url: &String = &env::var("DATABASE_MYSQL_URL").unwrap();

		Self {
			pool: MySqlPool::connect(url).await.unwrap()
		}
	}

	async fn execute(&self, query: &str) -> Result<MySqlQueryResult, sqlx::Error> {
		sqlx::query(query).execute(&self.pool).await
	}

	async fn fetch_optional(&self, query: &str) -> Option<MySqlRow> {
		let res = sqlx::query(query).fetch_optional(&self.pool).await;

		if res.is_ok() {
			res.unwrap()
		}
		else {
			None
		}
	}
}

pub struct MySqlDatabaseSetup {}

impl MySqlDatabaseSetup {
	pub async fn setup(db: &MySqlDatabase) -> Option<sqlx::Error> {
		let create_database_string = r"
		create database market_data;
	 	";
		let res = db.execute(create_database_string).await;

		if res.is_err() {
			return res.err();
		}

		let create_table_string = r"
		create table market_data.table_name
		(
			column_1 bigint
		);
	 	";
		let res = db.execute(create_table_string).await;

		if res.is_err() {
			return res.err();
		}

		None
	}
}
