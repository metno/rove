//! System for quality control of meteorological data.
//!
//! Provides a modular system for scheduling QC tests on data, and marshalling
//! data and metadata into these tests. It can be used as a standalone gRPC
//! service, or a component within another service (say, a data ingestor).
//!
//! As a standalone service:
//! ```no_run
//! use rove::{
//!     server::{ListenerType, start_server},
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
//!         ListenerType::Addr("[::1]:1337".parse()?),
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
//!     server::{ListenerType, RoveService},
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
//!     let rove = RoveService::new(construct_hardcoded_dag(), data_switch);
//!
//!     rove.validate_series_direct(
//!         String::from("test:single"),
//!         vec![String::from("dip_check"), String::from("step_check")],
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
//!     );
//!
//!     Ok(())
//! }
//! ```

pub mod data_switch;
mod harness;
pub mod server;

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
    use crate::data_switch::{
        self, DataConnector, GeoPoint, SeriesCache, SpatialCache, Timerange, Timestamp,
    };
    use async_trait::async_trait;
    use chronoutil::RelativeDuration;
    use dagmar::Dag;
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
            _polygon: Vec<GeoPoint>,
            _spatial_id: &str,
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

    pub fn construct_fake_dag() -> Dag<String> {
        let mut dag: Dag<String> = Dag::new();

        let test6 = dag.add_node(String::from("test6"));

        let test4 = dag.add_node_with_children(String::from("test4"), vec![test6]);
        let test5 = dag.add_node_with_children(String::from("test5"), vec![test6]);

        let test2 = dag.add_node_with_children(String::from("test2"), vec![test4]);
        let test3 = dag.add_node_with_children(String::from("test3"), vec![test5]);

        let _test1 = dag.add_node_with_children(String::from("test1"), vec![test2, test3]);

        dag
    }

    pub fn construct_hardcoded_dag() -> Dag<String> {
        let mut dag: Dag<String> = Dag::new();

        dag.add_node(String::from("dip_check"));
        dag.add_node(String::from("step_check"));
        dag.add_node(String::from("buddy_check"));
        dag.add_node(String::from("sct"));

        dag
    }
}
