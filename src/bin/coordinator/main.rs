use rove::coordinator::start_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_server().await
}
