extern crate core;

pub mod database;
pub mod trading_pair;
pub mod kline;
pub mod error;
pub mod api_connector;
pub mod logger;
pub mod cooldown_handler;
pub mod routine;
pub mod neural;

use std::fmt::{Debug, Formatter};
use futures::StreamExt;
use tch::{CModule, Device, IndexOp, nn, Reduction, Tensor, TrainableCModule};
use tch::Kind::{Double, Float};
use tch::nn::{Adam, LinearConfig, LSTM, Module, ModuleT, Optimizer, OptimizerConfig, RNN, RNNConfig, Sequential, SequentialT, VarStore};

use rand::seq::SliceRandom;
use sqlx::Row;
use tch::kind::FLOAT_CUDA;
use tch::Reduction::Mean;

use crate::api_connector::BinanceConnector;
use crate::database::analyzer::PostgresAbsenceAnalyser;
use crate::database::db::Database;
use crate::database::postgres::{PostgresDatabase, PostgresExecutor, PostgresSetup};
use crate::kline::Kline;

#[derive(Debug, Clone, Copy)]
struct Indicators {
	min: f32,
	max: f32,
	avg: f32
}

impl Indicators {
	fn as_array(&self) -> [f32; 3] {
		[self.min, self.max, self.avg]
	}
}

fn get_indicators(klines: &[Kline], min_price: f32, max_price: f32) -> Indicators {
	assert_ne!(klines.len(), 0);

	let mut min: f32 = klines.get(0).unwrap().low;
	let mut max: f32 = klines.get(0).unwrap().high;
	let mut avg: f32 = 0.0;
	let mut n: u32 = 0;

	for kline in klines {
		if kline.low < min {
			min = kline.low;
		}

		if kline.high > max {
			max = kline.high;
		}

		n += 1;
		// avg = (((n - 1) as f32) * avg + kline.open) / (n as f32);
		avg = avg + (kline.open - avg) / (n as f32);
	}

	Indicators {min: min / min_price, max: max / max_price, avg}
}

#[allow(dead_code)]
fn get_network_1(vs: &VarStore) -> Sequential {
	// 150 minutes, 5 data points for each timeframe
	// First layer is 150 * 5 = 750
	// Last layer is 3, one for each indicator: MIN, MAX, AVG
	let layer_counts = [750, 1500, 1500, 1500, 1500, 1500, 1500, 3];

	return nn::seq()
		.add(nn::linear(
			vs.root(),
			layer_counts[0],
			layer_counts[1],
			Default::default(),
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[1],
			layer_counts[2],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[2],
			layer_counts[3],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[3],
			layer_counts[4],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[4],
			layer_counts[5],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[5],
			layer_counts[6],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[6],
			layer_counts[7],
			Default::default()
		));
}

#[allow(dead_code)]
fn get_network_2(vs: &VarStore) -> SequentialT {
	// 150 minutes, 5 data points for each timeframe
	// First layer is 150 * 5 = 750
	// Last layer is 3, one for each indicator: MIN, MAX, AVG
	let layer_counts = [750, 7500, 7500, 7500, 7500, 3];

	return nn::seq_t()
		.add(nn::linear(
			vs.root(),
			layer_counts[0],
			layer_counts[1],
			Default::default(),
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[1],
			layer_counts[2],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[2],
			layer_counts[3],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[3],
			layer_counts[4],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[4],
			layer_counts[5],
			Default::default()
		));
}

#[allow(dead_code)]
fn get_network_3(vs: &VarStore) -> Sequential {
	// 150 minutes, 5 data points for each timeframe
	// First layer is 150 * 5 = 750
	// Last layer is 3, one for each indicator: MIN, MAX, AVG
	let layer_counts = [750, 7500, 7500, 7500, 7500, 7500, 7500, 7500, 7500, 3];

	return nn::seq()
		.add(nn::linear(
			vs.root(),
			layer_counts[0],
			layer_counts[1],
			Default::default(),
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[1],
			layer_counts[2],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[2],
			layer_counts[3],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[3],
			layer_counts[4],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[4],
			layer_counts[5],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[5],
			layer_counts[6],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[6],
			layer_counts[7],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[7],
			layer_counts[8],
			Default::default()
		))
		.add_fn(|xs| xs.relu())
		.add(nn::linear(
			vs.root(),
			layer_counts[8],
			layer_counts[9],
			Default::default()
		));
}

#[allow(dead_code)]
fn get_network(vs: &VarStore, layer_counts: &[i64], act_fn: fn(&Tensor) -> Tensor) -> SequentialT {
	// 150 minutes, 5 data points for each timeframe
	// First layer is 150 * 5 = 750
	// Last layer is 3, one for each indicator: MIN, MAX, AVG

	let mut net = nn::seq_t();

	for i in 1..layer_counts.len() {
		if i != 1 {
			net = net.add_fn(act_fn);
		}
		net = net.add(nn::linear(
			vs.root(),
			layer_counts[i - 1],
			layer_counts[i],
			Default::default(),
		));
	}

	net
}

#[allow(dead_code)]
fn get_network_lstm_1(vs: &VarStore) -> LSTM {
	// 150 minutes, 5 data points for each timeframe
	// First layer is 150 * 5 = 750
	// Last layer is 3, one for each indicator: MIN, MAX, AVG
	let layer_counts = [750, 7500, 7500, 7500, 7500, 7500, 7500, 7500, 7500, 3];

	return nn::lstm(vs.root(), 5, 15, RNNConfig::default());
}

fn get_average_f64(vec: &Vec<f64>) -> f64 {
	let mut avg: f64 = 0.0;
	let mut n: u32 = 0;

	for f in vec {
		n += 1;
		avg = avg + (f - avg) / (n as f64);
	}

	return avg;
}

fn get_min_f64(vec: &Vec<f64>) -> f64 {
	let mut min: f64 = vec.get(0).unwrap().clone();

	for &f in vec {
		if f < min {
			min = f;
		}
	}

	return min;
}

fn get_max_f64(vec: &Vec<f64>) -> f64 {
	let mut max: f64 = vec.get(0).unwrap().clone();

	for &f in vec {
		if f > max {
			max = f;
		}
	}

	return max;
}

async fn get_training_data(db: &PostgresDatabase, vs: &VarStore, offset: u32) -> Vec<(Tensor, Tensor)> {
	let mut training_data: Vec<(Tensor, Tensor)> = Vec::new();

	let query_string_fn = |x: u32| format!("
		SELECT *
		FROM klines_ethusdt
		ORDER BY time_open DESC
		LIMIT 180
		OFFSET 129600 - {x} * {offset};
	");

	// 90 days * 24 * 60 / 75 = 1728
	for i in 0..(90 * 24 * 60 / offset) /*1728*/ {
		let x: Vec<Kline> = sqlx::query(query_string_fn(i).as_str()).fetch_all(&db.pool).await.unwrap()
			.iter()
			.rev()
			.map(|row| {
				let k = Kline::new(row.get::<i64, usize>(0) as u64,
								   row.get::<f64, usize>(1) as f32,
								   row.get::<f64, usize>(2) as f32,
								   row.get::<f64, usize>(3) as f32,
								   row.get::<f64, usize>(4) as f32,
								   row.get::<f64, usize>(5) as f32,
								   row.get::<i32, usize>(6) as u64
				);

				k
			})
			.collect();

		let (inputs_nonspread, label_gen) = x.split_at(150);
		let mut inputs_spread: Vec<f32> = Vec::with_capacity(inputs_nonspread.len() * 5);
		let mut max_price = f32::MIN;
		let mut min_price = f32::MAX;

		assert_eq!(inputs_nonspread.len(), 150);
		assert_eq!(label_gen.len(), 30);

		for entry in inputs_nonspread {
			inputs_spread.push(entry.open);
			inputs_spread.push(entry.high);
			inputs_spread.push(entry.low);
			inputs_spread.push(entry.close);
			inputs_spread.push(entry.volume);
			// inputs_spread.push(entry.num_trades as f32);

			if entry.low < min_price {
				min_price = entry.low;
			}
			if entry.high > max_price {
				max_price = entry.high;
			}
		}

		let indicators = get_indicators(&label_gen, min_price, max_price);

		let l = Tensor::of_slice(inputs_spread.as_slice())
			// .view([1, inputs_spread.len()])
			.to_kind(Float)
			.to_device(vs.device());

		let r = Tensor::of_slice(indicators.as_array().as_slice())
			.view(3)
			.to_kind(Float)
			.to_device(vs.device());


		training_data.push((l, r));
	}

	training_data
}

fn learning_rate(epoch: u32) -> f64 {
	if epoch < 20 {
		return 1e-3;
	}
	else if epoch < 50 {
		return 1e-5;
	}
	else if epoch < 100 {
		return 1e-6;
	}

	return 1e-7;
}

async fn train_net(db: &PostgresDatabase, vs: &mut VarStore, net: &SequentialT, lr_fn: fn(u32) -> f64, offset: u32) {

	const INITIAL_LR: f64 = 1e-3;
	let mut opt = Adam::default().build(&vs, INITIAL_LR).unwrap();
	let mut training_data = get_training_data(db, vs, offset).await;
	let mut rng = rand::thread_rng();

	for epoch in 1..=400 {
		training_data.shuffle(&mut rng);
		let lr = lr_fn(epoch);
		opt.set_lr(lr);

		let mut losses: Vec<f64> = Vec::new();

		for data in training_data.iter() {
			let loss = net
				.forward_t(&data.0, true)
				.mse_loss(&data.1, Reduction::Mean);

			opt.backward_step(&loss);

			losses.push(f64::from(&loss));
		}

		let loss_avg = get_average_f64(&losses);
		let loss_min = get_min_f64(&losses);
		let loss_max = get_max_f64(&losses);

		println!(
			"epoch: {:4} \t lr: {lr:1.20} \t avg: {:8.5} \t min: {:8.5} \t max: {:8.5}",
			epoch,
			loss_avg,
			loss_min,
			loss_max,
		);
	}


	for data in training_data.iter() {
		let res = net.forward_t(&data.0, false);
		println!("In: {:?} Out: {:?} Exp: {} \n", &data.0.get(0), res, &data.1);
	}

	// vs.freeze();
	// let mut closure = |input: &[Tensor]| vec![net.forward_t(&input[0], false)];
	// let model = CModule::create_by_tracing(
	// 	"MyModule",
	// 	"forward",
	// 	&[Tensor::zeros(&[750i64], FLOAT_CUDA)],
	// 	&mut closure,
	// ).unwrap();
	// model.save("net4.pt").unwrap();
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

	//
	// Load training data
	//
	let mut vs = VarStore::new(Device::cuda_if_available());
	println!("cuda = {}", vs.device().is_cuda());

	//
	// Conventional Training
	//
	let layer_counts = [750, 500, 3];
	println!("layer_counts = {layer_counts:?}");
	fn lr_fn(epoch: u32) -> f64 {

		// Spiking of learning rate to escape local minima
		let base = f64::from({
			if epoch % 30 == 0 {
				7
			}
			else {
				1
			}
		});

		let epoch = f64::from(epoch);
		return base * 0.01f64 / 10f64.powf(epoch / 28.0);
	}
	let net = get_network(&vs, &layer_counts, |x| x.relu());
	train_net(&db, &mut vs, &net, lr_fn, 60).await;

	//
	// Loading and training again
	//
	// let mut net = TrainableCModule::load(Path::new("net.pt"), vs.root()).unwrap();
	// train_net(&db, &mut vs, &mut net, 75).await;
	// model.forward(&Tensor::zeros(&[750i64], FLOAT_CUDA));
}
