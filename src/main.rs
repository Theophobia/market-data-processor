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

fn get_indicators(klines: &[Kline]) -> Indicators {
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

	Indicators {min, max, avg}
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



async fn get_training_data_bare(db: &PostgresDatabase, offset: u32) -> Vec<(Vec<Kline>, Indicators)> {
	let mut training_data: Vec<(Vec<Kline>, Indicators)> = Vec::new();

	let query_string_fn = |x: u32| format!("
		SELECT *
		FROM klines_ethusdt
		ORDER BY time_open DESC
		LIMIT 180
		OFFSET 129600 - {x} * {offset};
	");

	// 90 days * 24 * 60 / 75 = 1728
	for i in 0..1728 {
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

		assert_eq!(inputs_nonspread.len(), 150);
		assert_eq!(label_gen.len(), 30);

		let inputs_nonspread: Vec<Kline> = inputs_nonspread.iter().map(|k| k.clone()).collect();

		let indicators = get_indicators(&label_gen);

		training_data.push((inputs_nonspread, indicators));
	}

	training_data
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
	for i in 0..1728 {
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

		assert_eq!(inputs_nonspread.len(), 150);
		assert_eq!(label_gen.len(), 30);

		for entry in inputs_nonspread {
			inputs_spread.push(entry.open);
			inputs_spread.push(entry.high);
			inputs_spread.push(entry.low);
			inputs_spread.push(entry.close);
			inputs_spread.push(entry.volume);
			// inputs_spread.push(entry.num_trades as f32);
		}

		let indicators = get_indicators(&label_gen);

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

async fn train_net(db: &PostgresDatabase, vs: &mut VarStore, net: &SequentialT, offset: u32) {

	const INITIAL_LR: f64 = 1e-3;
	let mut opt = Adam::default().build(&vs, INITIAL_LR).unwrap();
	let mut training_data = get_training_data(db, vs, offset).await;
	let mut rng = rand::thread_rng();

	for epoch in 1..=200 {
		training_data.shuffle(&mut rng);
		opt.set_lr(learning_rate(epoch));

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
			"epoch: {:4} avg: {:8.5} min: {:8.5} max: {:8.5}",
			epoch,
			loss_avg,
			loss_min,
			loss_max,
		);
	}

	// for data in training_data.iter() {
	// 	let res = net.forward(&data.0);
	// 	println!("In: {:?} Out: {:?} Exp: {} \n", &data.0.get(0), res, &data.1);
	// }

	vs.freeze();
	let mut closure = |input: &[Tensor]| vec![net.forward_t(&input[0], false)];
	let model = CModule::create_by_tracing(
		"MyModule",
		"forward",
		&[Tensor::zeros(&[750i64], FLOAT_CUDA)],
		&mut closure,
	).unwrap();
	model.save("net2.pt").unwrap();
}


#[derive(Debug)]
struct MyLSTM {
	lstm: LSTM,
}

impl MyLSTM {
	fn new(vs: &VarStore, input_size: i64, hidden_size: i64) -> MyLSTM {
		let lstm = nn::lstm(vs.root(), input_size, hidden_size, Default::default());
		MyLSTM { lstm }
	}
}

impl Module for MyLSTM {
	fn forward(&self, input: &Tensor) -> Tensor {
		self.lstm.seq(input).0
	}
}

#[derive(Debug)]
struct MyRNN {
	lstm: MyLSTM,
	seq: SequentialT,
}

impl MyRNN {
	fn new(vs: &VarStore, input_size: i64, hidden_size: i64, output_size: i64) -> MyRNN {
		let lstm = MyLSTM::new(vs, input_size, hidden_size);
		let seq = nn::seq_t()
			.add(nn::linear(vs.root(), hidden_size, output_size, Default::default()))
			.add_fn(|xs| xs.relu());

		MyRNN { lstm, seq }
	}

	fn train(&self, input: &Tensor, label: &Tensor, opt: &mut Optimizer) -> f64 {
		let output = self.lstm.forward(input);
		let loss = self.seq.forward_t(&output, true).mse_loss(label, Mean);
		opt.backward_step(&loss);

		f64::from(&loss)
	}
}

impl Module for MyRNN {
	fn forward(&self, input: &Tensor) -> Tensor {
		let output = self.lstm.forward(input);
		self.seq.forward_t(&output, false)
	}
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
	println!("cuda: {}", vs.device().is_cuda());

	// Create an instance of the RNN model
	let input_size = 5; // Number of input features
	let hidden_size = 64; // Number of hidden units in the LSTM layer
	let output_size = 3; // Number of output values
	let rnn = MyRNN::new(&vs, input_size, hidden_size, output_size);

	// Generate a sample input tensor
	let seq_len = 1728; // Number of batches
	let batch_size = 150; // Number of samples in a batch
	let input = Tensor::zeros(&[seq_len, batch_size, input_size], (Float, vs.device()));
	let label = Tensor::zeros(&[seq_len, output_size], (Float, vs.device()));

	println!("input = {input}");

	let training_data = get_training_data_bare(&db, 75).await;

	//
	// Fill "input" and "label"
	//
	let mut i: i64 = 0;
	for data in training_data {
		if i >= seq_len {
			break;
		}

		let mut j: i64 = 0;

		// println!("data.0.len = {}", data.0.len());
		while (j as usize) < data.0.len() {
			let new_data = data.0.get(j as usize).unwrap();

			let _ = input.i((i, j, 0)).fill_(new_data.open as f64);
			let _ = input.i((i, j, 1)).fill_(new_data.high as f64);
			let _ = input.i((i, j, 2)).fill_(new_data.low as f64);
			let _ = input.i((i, j, 3)).fill_(new_data.close as f64);
			let _ = input.i((i, j, 4)).fill_(new_data.volume as f64);

			j += 1;
		}

		// Modify labels tensor to be indicators
		let _ = label.i((i, 0)).fill_(data.1.min as f64);
		let _ = label.i((i, 1)).fill_(data.1.max as f64);
		let _ = label.i((i, 2)).fill_(data.1.avg as f64);

		i += 1;
	}

	println!("input = {input}");
	println!("label = {label}");

	let mut opt = Adam::default().build(&vs, 1e-3).unwrap();

	for epoch in 0..1000 {
		for batch_idx in 0..11 {
			let start_idx = batch_idx * batch_size;
			let end_idx = (batch_idx + 1) * batch_size;

			let input_batch = input.narrow(0, start_idx as i64, batch_size as i64).to_device(vs.device());
			let label_batch = label.narrow(0, start_idx as i64, batch_size as i64).to_device(vs.device());
			// println!("input_batch = {input_batch}");
			// println!("label_batch = {label_batch}");

			let (output, _) = rnn.lstm.lstm.seq(&input_batch);
			// println!("Output = {output}");
			let output = output.select(0, output.size()[0] - 1);
			// println!("Output = {output}");
			let output = rnn.seq.forward_t(&output, true);
			// println!("Output = {output}");
			// println!("Label = {label}");

			let loss = output.mse_loss(&label_batch, Mean);
			opt.backward_step(&loss);
			println!("Epoch {epoch}: Loss = {loss}");
		}
	}

	// Run the RNN model on the input tensor
	// let output = rnn.forward(&input);

	//
	// Conventional Training
	//
	// let net = get_network_2(&vs);
	// train_net(&db, &mut vs, &net, 75).await;

	//
	// Loading and training again
	//
	// let mut net = TrainableCModule::load(Path::new("net.pt"), vs.root()).unwrap();
	// train_net(&db, &mut vs, &mut net, 75).await;
	// model.forward(&Tensor::zeros(&[750i64], FLOAT_CUDA));
}
