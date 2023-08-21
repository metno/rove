use frost::Frost;
use lustre_netatmo::LustreNetatmo;
use rove::{
    data_switch::{DataSource, DataSwitch},
    server::{start_server, ListenerType},
};
use std::collections::HashMap;

// TODO: use anyhow for error handling?
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_switch = DataSwitch::new(HashMap::from([
        ("frost", &Frost as &dyn DataSource),
        ("lustre_netatmo", &LustreNetatmo as &dyn DataSource),
    ]));

    start_server(ListenerType::Addr("[::1]:1337".parse()?), data_switch).await
}
