use std::fmt::{Display, Formatter};
use std::fs::{create_dir, File};
use std::io::Write;
use std::sync::Mutex;
use chrono::Utc;
use once_cell::sync::Lazy;

static LOGGER: Lazy<Mutex<Logger>> = Lazy::new(|| Mutex::new(Logger::new()));
const IS_ALSO_LOGGING_TO_CONSOLE: bool = true;

pub enum LogLevel {
	FINE,
	INFO,
	ERROR,
}

impl Display for LogLevel {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", match &self {
			LogLevel::FINE => {"FIN"}
			LogLevel::INFO => {"NFO"}
			LogLevel::ERROR => {"ERR"}
		})
	}
}

pub struct Logger {
	output_file: File
}

impl Logger {
	pub fn new() -> Self {
		if create_dir("log").is_err() {
			// already exists, or permission error
		}

		let f = File::create(format!("log/{}.txt", Utc::now().format("%Y-%m-%d_%H-%M-%S"))).unwrap();

		Self {output_file: f}
	}

	pub fn log(log_level: LogLevel, channel: &String, message: &String) {
		let time = Utc::now().format("%Y-%m-%d_%H-%M-%S_%3f");

		let full_message = format!("[{time}] [{log_level}] [{channel}] : {message}\n");
		let res = LOGGER.lock().unwrap().output_file.write_all(full_message.as_bytes());

		if IS_ALSO_LOGGING_TO_CONSOLE {
			print!("{full_message}");
		}

		res.unwrap();
	}

	pub fn log_str(log_level: LogLevel, channel: &str, message: &str) {
		Self::log(log_level, &String::from(channel), &String::from(message));
	}
}
