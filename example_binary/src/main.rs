use clap::Parser;
use example_binary::construct_hardcoded_dag;
use frost::Frost;
use lustre_netatmo::LustreNetatmo;
use rove::{
    data_switch::{DataSource, DataSwitch},
    server::{start_server, ListenerType},
};
use std::collections::HashMap;
use tracing::Level;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from("[::1]:1337"))]
    address: String,
    #[arg(short = 'l', long, default_value_t = Level::INFO)]
    max_trace_level: Level,
}

// TODO: use anyhow for error handling?
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(args.max_trace_level)
        .init();

    let data_switch = DataSwitch::new(HashMap::from([
        ("frost", &Frost as &dyn DataSource),
        ("lustre_netatmo", &LustreNetatmo as &dyn DataSource),
    ]));

    start_server(
        ListenerType::Addr(args.address.parse()?),
        data_switch,
        construct_hardcoded_dag(),
    )
    .await
}