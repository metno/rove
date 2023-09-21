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
