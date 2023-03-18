pub mod database;

use serde::{Deserialize, Serialize};
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresDatabaseSetup};

#[allow(dead_code)]
#[derive(Deserialize)]
struct KlinePreProcess {
	time_open: u64,
	open: String,
	high: String,
	low: String,
	close: String,
	volume: String,
	time_close: u64,
	quote_asset_volume: String,
	num_trades: u64,
	taker_buy_base_asset_volume: String,
	taker_buy_base_quote_volume: String,
	unused: String,
}

#[allow(dead_code)]
impl KlinePreProcess {
	pub fn process(&self) -> Kline {
		let time_open = self.time_open;
		let open = self.open.parse::<f32>().unwrap();
		let high = self.high.parse::<f32>().unwrap();
		let low = self.low.parse::<f32>().unwrap();
		let close = self.close.parse::<f32>().unwrap();
		let volume = self.volume.parse::<f32>().unwrap();
		let num_trades = self.num_trades;

		Kline::new(time_open, open, high, low, close, volume, num_trades)
	}
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
struct Kline {
	time_open: u64,
	open: f32,
	high: f32,
	low: f32,
	close: f32,
	volume: f32,
	num_trades: u64,
}

#[allow(dead_code)]
impl Kline {
	pub fn new(time_open: u64, open: f32, high: f32, low: f32, close: f32, volume: f32, num_trades: u64) -> Self {
		Self { time_open, open, high, low, close, volume, num_trades }
	}
}


#[tokio::main]
async fn main() {
	dotenv::dotenv().unwrap();

	// let db: SqliteDatabase = SqliteDatabase::new().await;
	// SqliteDatabaseSetup::setup(&db).await.unwrap();

	let db = PostgresDatabase::new().await;
	PostgresDatabaseSetup::setup(&db).await;
	let missing: Vec<u64> = PostgresAbsenceAnalyser::analyze(&db).await;
	println!("Memory taken: {}", std::mem::size_of::<u64>());

	// let pool: Pool<Sqlite> = SqlitePool::connect(&env::var("DATABASE_URL").unwrap()).await.unwrap();
	//
	// let res = sqlx::query(r"
	// create table if not exists klines
	// (
	// 	time_open integer not null
	// 		constraint table_name_pk
	// 			primary key,
	// 	open real not null,
	// 	high real not null,
	// 	low real not null,
	// 	close real not null,
	// 	volume real not null,
	// 	num_trades integer not null
	// );
	//
	// create unique index if not exists klines_time_open_uindex
	// 	on klines (time_open);
	// ").execute(&pool).await.unwrap();

	// let files: ReadDir = fs::read_dir("data").unwrap();
	// let files_count = fs::read_dir("data").unwrap().count();
	// let mut file_number = 0;
	//
	// for file_wrapped in files {
	// 	file_number += 1;
	// 	println!("File {}/{}", file_number, files_count);
	//
	// 	let file_unwrapped = file_wrapped.unwrap();
	//
	// 	let file: File = File::open(file_unwrapped.path()).unwrap();
	// 	let reader = BufReader::new(file);
	//
	// 	let json: Vec<KlinePreProcess> = serde_json::from_reader(reader).unwrap();
	// 	println!("Klines {}", json.len());
	//
	// 	let mut transaction = String::with_capacity(200_000);
	// 	transaction.push_str("begin transaction;\n");
	//
	// 	for kline_preprocess in json {
	// 		let kline = kline_preprocess.process();
	//
	// 		let query = format!(r"
	// 		INSERT INTO klines (time_open, open, high, low, close, volume, num_trades)
	// 		SELECT {}, {}, {}, {}, {}, {}, {}
	// 		WHERE NOT EXISTS (
	// 			SELECT 1 FROM klines WHERE time_open = {}
	// 		);", kline.time_open, kline.open, kline.high, kline.low, kline.close, kline.volume, kline.num_trades, kline.time_open);
	//
	// 		transaction.push_str(query.as_str());
	// 		transaction.push('\n');
	// 	}
	// 	transaction.push_str("commit;");
	// 	// println!("{}", transaction);
	//
	// 	sqlx::query(transaction.as_str()).execute(&pool).await.unwrap();
	// }

	// let mut curr_time = 1568887320000u64;
	// let stop_time = 1678978440000u64;
	//
	// while curr_time <= stop_time {
	// 	let query = format!("SELECT 1 FROM klines WHERE time_open = {};", curr_time);
	// 	let res = sqlx::query(query.as_str()).fetch_optional(&pool).await;
	//
	// 	if !(res.is_ok() && res.unwrap().is_some()) {
	// 		println!("{curr_time}");
	// 	}
	//
	// 	curr_time += 60000000u64;
	// }
}
