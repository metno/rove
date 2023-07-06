use rove::{coordinator::start_server, util::ListenerType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_server(ListenerType::Addr("[::1]:1337".parse()?)).await
}
