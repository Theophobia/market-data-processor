use std::sync::Mutex;
use chrono::Utc;
use once_cell::sync::Lazy;
use crate::logger::logger::{Logger, LogLevel};

static COOLDOWN_HANDLER: Lazy<Mutex<CooldownHandler>> = Lazy::new(|| Mutex::new(CooldownHandler::new()));

pub struct CooldownHandler {
	until: Option<i64>,
}

impl CooldownHandler {
	pub fn new() -> Self {
		Self { until: None }
	}

	pub fn can_request() -> bool {
		let mut cdh = COOLDOWN_HANDLER.lock().unwrap();

		if cdh.until.is_none() {
			return true;
		}

		let until = cdh.until.unwrap();
		let now = Utc::now().timestamp_millis();

		if until < now {
			cdh.until = None;
			return true;
		}

		return false;
	}

	pub fn set_cooldown_to(until: i64) {
		let mut cdh = COOLDOWN_HANDLER.lock().unwrap();

		if cdh.until.is_some() {
			cdh.until = Some(std::cmp::max(cdh.until.unwrap(), until));

			Logger::log_str(
				LogLevel::FINE,
				"set_cooldown_to() cooldown_handler.rs",
				format!("Set cooldown until {} (note: already had cooldown set, chose the max)", cdh.until.unwrap()).as_str()
			);
		} else {
			cdh.until = Some(until);

			Logger::log_str(
				LogLevel::FINE,
				"set_cooldown_to() cooldown_handler.rs",
				format!("Set cooldown until {}", cdh.until.unwrap()).as_str()
			);
		}
	}

	pub fn get_until() -> Option<i64> {
		let cdh = COOLDOWN_HANDLER.lock().unwrap();
		cdh.until
	}
}
