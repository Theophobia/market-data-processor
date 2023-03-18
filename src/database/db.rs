use async_trait::async_trait;

#[async_trait]
pub trait Database<A, B> {
	async fn new() -> Self;
	async fn execute(&self, query: &str) -> Result<A, sqlx::Error>;
	async fn fetch_optional(&self, query: &str) -> Option<B>;
}

