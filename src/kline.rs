use serde::{Deserialize, Serialize};
use tch::nn::VarStore;
use tch::{Kind, Tensor};

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct KlinePreProcess {
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Kline {
	pub time_open: u64,
	pub open: f32,
	pub high: f32,
	pub low: f32,
	pub close: f32,
	pub volume: f32,
	pub num_trades: u64,
}

#[allow(dead_code)]
impl Kline {
	pub fn new(time_open: u64, open: f32, high: f32, low: f32, close: f32, volume: f32, num_trades: u64) -> Self {
		Self { time_open, open, high, low, close, volume, num_trades }
	}

	pub fn as_ohlcv_tensor(&self, vs: &VarStore, kind: Kind) -> Tensor {
		Tensor::of_slice(&[
			self.open.clone(),
			self.high.clone(),
			self.low.clone(),
			self.close.clone(),
			self.volume.clone()
		])
			.to_kind(kind)
			.to_device(vs.device())
	}
}
