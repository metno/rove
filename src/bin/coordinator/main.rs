use rove::{coordinator::start_server, util::ListenerType};
use tonic::transport::Endpoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_server(
        ListenerType::Addr("[::1]:1337".parse()?),
        Endpoint::try_from("[::1]:1338")?,
    )
    .await
}
