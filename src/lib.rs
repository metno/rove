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
//!         vec!["dip_check", "step_check"],
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
//#![warn(missing_docs)]

mod dag;
pub mod data_switch;
mod harness;
#[warn(missing_docs)]
mod scheduler;
mod server;

pub use dag::Dag;

pub use scheduler::Scheduler;

/// Starts up a gRPC server to process QC run requests
///
/// Takes a [socket address](std::net::SocketAddr) to listen on, a
/// [data switch](data_switch::DataSwitch) to provide access to data sources,
/// and a [dag](dag::Dag) to encode dependencies between tests
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
        data_switch::{
            self, DataConnector, GeoPoint, SeriesCache, SpatialCache, Timerange, Timestamp,
        },
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
        async fn get_series_data(
            &self,
            data_id: &str,
            _timespec: Timerange,
            num_leading_points: u8,
        ) -> Result<SeriesCache, data_switch::Error> {
            match data_id {
                "single" => black_box(Ok(SeriesCache {
                    start_time: Timestamp(0),
                    period: RelativeDuration::minutes(5),
                    data: vec![Some(1.); self.data_len_single],
                    num_leading_points,
                })),
                "series" => black_box(Ok(SeriesCache {
                    start_time: Timestamp(0),
                    period: RelativeDuration::minutes(5),
                    data: vec![Some(1.); self.data_len_spatial],
                    num_leading_points,
                })),
                _ => panic!("unknown data_id"),
            }
        }

        async fn get_spatial_data(
            &self,
            _data_id: &str,
            _polygon: Vec<GeoPoint>,
            _timestamp: Timestamp,
        ) -> Result<SpatialCache, data_switch::Error> {
            black_box(Ok(SpatialCache::new(
                (0..self.data_len_spatial)
                    .map(|i| ((i as f32).powi(2) * 0.001) % 3.)
                    .collect(),
                (0..self.data_len_spatial)
                    .map(|i| ((i as f32 + 1.).powi(2) * 0.001) % 3.)
                    .collect(),
                vec![1.; self.data_len_spatial],
                vec![1.; self.data_len_spatial],
            )))
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
