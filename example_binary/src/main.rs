use rove::{
    coordinator::start_server,
    data_switch::{frost, DataSource, DataSwitch},
    util::ListenerType,
};
use std::collections::HashMap;

// TODO: use anyhow for error handling
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_switch = DataSwitch::new(HashMap::from([("frost", &frost::Frost as &dyn DataSource)]));

    start_server(ListenerType::Addr("[::1]:1337".parse()?), data_switch).await
}
