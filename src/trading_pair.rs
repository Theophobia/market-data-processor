use std::fmt::{Display, Formatter};

#[allow(non_camel_case_types)]
#[derive(Eq, PartialEq, Hash, Clone, Copy, Debug)]
pub enum TradingPair {
	BTCUSDT,
}

impl Display for TradingPair {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let s: &str = match self {
			TradingPair::BTCUSDT => {"BTCUSDT"}
		};

		write!(f, "{s}")
	}
}
