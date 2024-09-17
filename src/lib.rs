//! System for quality control of meteorological data.
//!
//! Provides a modular system for scheduling QC tests on data, and marshalling
//! data and metadata into these tests. It can be used as a standalone gRPC
//! service, or a component within another service (say, a data ingestor).
//!
//! As a standalone service:
//! ```no_run
//! use rove::{
//!     start_server,
//!     data_switch::{DataSwitch, DataConnector},
//!     dev_utils::{TestDataSource, construct_hardcoded_dag},
//! };
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let data_switch = DataSwitch::new(HashMap::from([
//!         ("test", &TestDataSource{
//!             data_len_single: 3,
//!             data_len_series: 1000,
//!             data_len_spatial: 1000,
//!         } as &dyn DataConnector),
//!     ]));
//!
//!     start_server(
//!         "[::1]:1337".parse()?,
//!         data_switch,
//!         construct_hardcoded_dag(),
//!     )
//!     .await
//! }
//! ```
//!
//! As a component:
//! ```no_run
//! use rove::{
//!     Scheduler,
//!     data_switch::{DataSwitch, DataConnector, Timestamp, Timerange},
//!     dev_utils::{TestDataSource, construct_hardcoded_dag},
//! };
//! use std::collections::HashMap;
//! use chrono::{Utc, TimeZone};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let data_switch = DataSwitch::new(HashMap::from([
//!         ("test", &TestDataSource{
//!             data_len_single: 3,
//!             data_len_series: 1000,
//!             data_len_spatial: 1000,
//!         } as &dyn DataConnector),
//!     ]));
//!
//!     let rove_scheduler = Scheduler::new(construct_hardcoded_dag(), data_switch);
//!
//!     let mut rx = rove_scheduler.validate_series_direct(
//!         "test:single",
//!         &["dip_check", "step_check"],
//!         Timerange{
//!             start: Timestamp(
//!                 Utc.with_ymd_and_hms(2023, 6, 26, 12, 0, 0)
//!                     .unwrap()
//!                     .timestamp(),
//!             ),
//!             end: Timestamp(
//!                 Utc.with_ymd_and_hms(2023, 6, 26, 14, 0, 0)
//!                     .unwrap()
//!                     .timestamp(),
//!             ),
//!         },
//!     ).await?;
//!
//!     while let Some(response) = rx.recv().await {
//!         match response {
//!             Ok(inner) => {
//!                 println!("\ntest name: {}\n", inner.test);
//!                 for result in inner.results {
//!                     println!("timestamp: {}", result.time.unwrap().seconds);
//!                     println!("flag: {}", result.flag);
//!                 }
//!             }
//!             Err(e) => println!("uh oh, got an error: {}", e),
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

#![warn(missing_docs)]

mod dag;
pub mod data_switch;
mod harness;
mod scheduler;
mod server;

pub use dag::Dag;

pub use scheduler::Scheduler;

pub use server::start_server;

#[doc(hidden)]
pub use server::start_server_unix_listener;

pub(crate) mod pb {
    tonic::include_proto!("rove");

    impl TryFrom<olympian::Flag> for Flag {
        type Error = String;

        fn try_from(item: olympian::Flag) -> Result<Self, Self::Error> {
            match item {
                olympian::Flag::Pass => Ok(Self::Pass),
                olympian::Flag::Fail => Ok(Self::Fail),
                olympian::Flag::Warn => Ok(Self::Warn),
                olympian::Flag::Inconclusive => Ok(Self::Inconclusive),
                olympian::Flag::Invalid => Ok(Self::Invalid),
                olympian::Flag::DataMissing => Ok(Self::DataMissing),
                olympian::Flag::Isolated => Ok(Self::Isolated),
                _ => Err(format!("{:?}", item)),
            }
        }
    }
}

#[doc(hidden)]
pub mod dev_utils {
    use crate::{
        data_switch::{self, DataCache, DataConnector, SpaceSpec, Timerange, Timestamp},
        Dag,
    };
    use async_trait::async_trait;
    use chronoutil::RelativeDuration;
    use std::hint::black_box;

    #[derive(Debug)]
    pub struct TestDataSource {
        pub data_len_single: usize,
        pub data_len_series: usize,
        pub data_len_spatial: usize,
    }

    #[async_trait]
    impl DataConnector for TestDataSource {
        async fn fetch_data(
            &self,
            space_spec: SpaceSpec<'_>,
            _time_spec: Timerange,
            num_leading_points: u8,
            num_trailing_points: u8,
            _extra_spec: Option<&str>,
        ) -> Result<DataCache, data_switch::Error> {
            match space_spec {
                SpaceSpec::One(data_id) => match data_id {
                    // TODO: should we maybe be using time_spec for these instead of data_id?
                    // maybe something to come back to when we finalize the format of time_spec
                    "single" => black_box(Ok(DataCache::new(
                        vec![0.; 1],
                        vec![0.; 1],
                        vec![0.; 1],
                        Timestamp(0),
                        RelativeDuration::minutes(5),
                        num_leading_points,
                        num_trailing_points,
                        vec![vec![Some(1.); self.data_len_single]; 1],
                    ))),
                    "series" => black_box(Ok(DataCache::new(
                        vec![0.; 1],
                        vec![0.; 1],
                        vec![0.; 1],
                        Timestamp(0),
                        RelativeDuration::minutes(5),
                        num_leading_points,
                        num_trailing_points,
                        vec![vec![Some(1.); self.data_len_spatial]; 1],
                    ))),
                    _ => panic!("unknown data_id"),
                },
                SpaceSpec::All => black_box(Ok(DataCache::new(
                    (0..self.data_len_spatial)
                        .map(|i| ((i as f32).powi(2) * 0.001) % 3.)
                        .collect(),
                    (0..self.data_len_spatial)
                        .map(|i| ((i as f32 + 1.).powi(2) * 0.001) % 3.)
                        .collect(),
                    vec![1.; self.data_len_spatial],
                    Timestamp(0),
                    RelativeDuration::minutes(5),
                    // TODO: update this to use num_leading/trailing?
                    0,
                    0,
                    vec![vec![Some(1.); 1]; self.data_len_spatial],
                ))),
                SpaceSpec::Polygon(_) => unimplemented!(),
            }
        }
    }

    pub fn construct_fake_dag() -> Dag<&'static str> {
        let mut dag: Dag<&'static str> = Dag::new();

        let test6 = dag.add_node("test6");

        let test4 = dag.add_node_with_children("test4", vec![test6]);
        let test5 = dag.add_node_with_children("test5", vec![test6]);

        let test2 = dag.add_node_with_children("test2", vec![test4]);
        let test3 = dag.add_node_with_children("test3", vec![test5]);

        let _test1 = dag.add_node_with_children("test1", vec![test2, test3]);

        dag
    }

    pub fn construct_hardcoded_dag() -> Dag<&'static str> {
        let mut dag: Dag<&'static str> = Dag::new();

        dag.add_node("dip_check");
        dag.add_node("step_check");
        dag.add_node("buddy_check");
        dag.add_node("sct");

        dag
    }
}
