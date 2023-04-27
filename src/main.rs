pub mod database;
pub mod trading_pair;
pub mod kline;
pub mod error;
pub mod api_connector;
pub mod logger;
pub mod cooldown_handler;
pub mod routine;
pub mod neural;

use tch::{Device, nn, Reduction, Tensor};
use tch::Kind::{Double, Float};
use tch::nn::{Adam, LinearConfig, Module, ModuleT, OptimizerConfig};

use rand::seq::SliceRandom;

use crate::api_connector::BinanceConnector;
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresExecutor, PostgresSetup};

fn xor_many(values: &[f64]) -> f64 {
	if values.len() == 0 {
		return 0.0;
	}

	let mut lhs = values[0];

	for i in 1..values.len() {
		lhs = xor(lhs, values[i]);
	}

	return lhs;
}

fn xor(x: f64, y: f64) -> f64 {
	let x: bool = {
		if x > 0.0 {
			true
		} else {
			false
		}
	};

	let y: bool = {
		if y > 0.0 {
			true
		} else {
			false
		}
	};

	return f64::from((x || y) && (!(x && y)));
}

#[tokio::main]
async fn main() {
	use crate::trading_pair::TradingPair::*;

	dotenv::dotenv().unwrap();

	let pairs = vec![BTCUSDT, LDOUSDT, ETHUSDT];
	let db = PostgresDatabase::new().await;
	let api_con = BinanceConnector::new();

	PostgresSetup::setup(&db, &pairs).await;

	// for pair in pairs.iter() {
	// 	let _ = PostgresExecutor::fetch_insert_leading_trailing(&db, &api_con, pair).await;
	// 	let _ = PostgresExecutor::update_possible_open_times(&db, &api_con, pair).await;
	// }
	//
	// let _missing = PostgresAbsenceAnalyser::analyze(&db, &pairs).await;

	let vs = nn::VarStore::new(Device::cuda_if_available());

	let mut training_data: Vec<(Tensor, Tensor)> = Vec::new();

	for x1 in 0..=1 {
		for x2 in 0..=1 {
			for x3 in 0..=1 {
				for x4 in 0..=1 {
					let y = xor_many(&[x1 as f64, x2 as f64, x3 as f64, x4 as f64]);

					let l = Tensor::of_slice(&[x1, x2, x3, x4])
						.view([1, 4])
						.to_kind(Float)
						.to_device(Device::cuda_if_available());

					let r = Tensor::of_slice(&[y])
						.view(1)
						.to_kind(Float)
						.to_device(Device::cuda_if_available());

					training_data.push((l, r))
				}
			}
		}
	}

	let net = nn::seq()
		.add(nn::linear(
			vs.root(),
			4,
			32,
			LinearConfig::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			32,
			32,
			LinearConfig::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			32,
			1,
			LinearConfig::default()
		));

	const INITIAL_LR: f64 = 1e-2;
	let mut opt = Adam::default().build(&vs, INITIAL_LR).unwrap();

	let mut rng = rand::thread_rng();
	for epoch in 1..=1000 {
		training_data.shuffle(&mut rng);
		opt.set_lr(INITIAL_LR / (epoch as f64));

		for data in training_data.iter() {
			let loss = net
				.forward(&data.0)
				.mse_loss(&data.1, Reduction::Mean);

			opt.backward_step(&loss);

			let test_accuracy = net
				.forward(&data.0)
				.accuracy_for_logits(&data.1);

			println!("Epoch {epoch}");
			// println!(
			// 	"epoch: {:4} train loss: {:8.5} test acc: {:5.2}%",
			// 	epoch,
			// 	f64::from(&loss),
			// 	100.0 * f64::from(&test_accuracy),
			// );
		}
	}

	for data in training_data.iter() {
		let res = net.forward(&data.0);
		println!("In: {:?} Out: {:?} Exp: {} \n", &data.0.get(0), res.get(0), &data.1.get(0));
	}


	// 120 minutes, 6 data points for each timeframe
	// First layer is 120 * 6 = 720
	// Second layer is 120 * 6 * 3 = 2160
	// Third layer is 120 * 6 = 720
	// Fourth layer is 120
	// Last layer is 3, indicator count
	// let layer_counts = [720, 2160, 720, 120, 3];

	// let net = nn::seq()
	// 	.add(nn::linear(
	// 		vs.root(),
	// 		layer_counts[0],
	// 		layer_counts[1],
	// 		Default::default(),
	// 	))
	// 	.add_fn(|xs| xs.leaky_relu())
	// 	.add(nn::linear(
	// 		vs.root(),
	// 		layer_counts[1],
	// 		layer_counts[2],
	// 		Default::default()
	// 	))
	// 	.add_fn(|xs| xs.leaky_relu())
	// 	.add(nn::linear(
	// 		vs.root(),
	// 		layer_counts[2],
	// 		layer_counts[3],
	// 		Default::default()
	// 	))
	// 	.add_fn(|xs| xs.leaky_relu())
	// 	.add(nn::linear(
	// 		vs.root(),
	// 		layer_counts[3],
	// 		layer_counts[4],
	// 		Default::default()
	// 	));
	//
	// let mut opt = Adam::default().build(&vs, 1e-3).unwrap();
	// let m = tch::vision::mnist::load_dir("data").unwrap();
	//
	// for epoch in 1..=10 {
	// 	let loss = net
	// 		.forward(&m.train_images)
	// 		.cross_entropy_for_logits(&m.train_labels);
	//
	// 	opt.backward_step(&loss);
	//
	// 	let test_accuracy = net
	// 		.forward(&m.test_images)
	// 		.accuracy_for_logits(&m.test_labels);
	//
	// 	println!(
	// 		"epoch: {:4} train loss: {:8.5} test acc: {:5.2}%",
	// 		epoch,
	// 		f64::from(&loss),
	// 		100.0 * f64::from(&test_accuracy),
	// 	);
	// }

	println!("cuda? {}", vs.device().is_cuda());
}
