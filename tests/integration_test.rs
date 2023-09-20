use async_trait::async_trait;
use chronoutil::RelativeDuration;
use core::future::Future;
use dagmar::Dag;
use pb::{rove_client::RoveClient, Flag, ValidateSeriesRequest, ValidateSpatialRequest};
use rove::{
    data_switch::{
        self, DataConnector, DataSwitch, GeoPoint, SeriesCache, SpatialCache, Timerange, Timestamp,
    },
    server::{start_server, ListenerType},
};
use std::{collections::HashMap, sync::Arc};
use tempfile::NamedTempFile;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::{wrappers::UnixListenerStream, StreamExt};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

mod pb {
    tonic::include_proto!("rove");
}

const DATA_LEN_SINGLE: usize = 3;
const DATA_LEN_SPATIAL: usize = 1000;

#[derive(Debug)]
struct TestDataSource;

#[async_trait]
impl DataConnector for TestDataSource {
    async fn get_series_data(
        &self,
        _data_id: &str,
        _timespec: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, data_switch::Error> {
        Ok(SeriesCache {
            start_time: Timestamp(0),
            period: RelativeDuration::minutes(5),
            data: vec![Some(1.); DATA_LEN_SINGLE],
            num_leading_points,
        })
    }

    async fn get_spatial_data(
        &self,
        _polygon: Vec<GeoPoint>,
        _spatial_id: &str,
        _timestamp: Timestamp,
    ) -> Result<SpatialCache, data_switch::Error> {
        Ok(SpatialCache::new(
            (0..DATA_LEN_SPATIAL)
                .map(|i| ((i as f32).powi(2) * 0.001) % 3.)
                .collect(),
            (0..DATA_LEN_SPATIAL)
                .map(|i| ((i as f32 + 1.).powi(2) * 0.001) % 3.)
                .collect(),
            vec![1.; DATA_LEN_SPATIAL],
            vec![1.; DATA_LEN_SPATIAL],
        ))
    }
}

fn construct_fake_dag() -> Dag<String> {
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

pub async fn set_up_rove(
    data_switch: DataSwitch<'static>,
    dag: Dag<String>,
) -> (impl Future<Output = ()>, RoveClient<Channel>) {
    let coordintor_socket = NamedTempFile::new().unwrap();
    let coordintor_socket = Arc::new(coordintor_socket.into_temp_path());
    std::fs::remove_file(&*coordintor_socket).unwrap();
    let coordintor_uds = UnixListener::bind(&*coordintor_socket).unwrap();
    let coordintor_stream = UnixListenerStream::new(coordintor_uds);
    let coordinator_future = async {
        start_server(
            ListenerType::UnixListener(coordintor_stream),
            data_switch,
            dag,
        )
        .await
        .unwrap();
    };

    let coordinator_channel = Endpoint::try_from("http://any.url")
        .unwrap()
        .connect_with_connector(service_fn(move |_: tonic::transport::Uri| {
            let socket = Arc::clone(&coordintor_socket);
            async move { UnixStream::connect(&*socket).await }
        }))
        .await
        .unwrap();
    let client = RoveClient::new(coordinator_channel);

    (coordinator_future, client)
}

// This test exists because the real dag isn't currently complex enough to
// verify correct scheduling. In the future we will either decide we don't
// need complex dag based scheduling, or the real dag will become complex.
// In either case this test will eventually be obsolete, so:
// TODO: delete
#[tokio::test]
async fn integration_test_fake_dag() {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::INFO)
    //     .init();

    let data_switch = DataSwitch::new(HashMap::from([(
        "test",
        &TestDataSource as &dyn DataConnector,
    )]));

    let (coordinator_future, mut client) = set_up_rove(data_switch, construct_fake_dag()).await;

    let request_future = async {
        let mut stream = client
            .validate_series(ValidateSeriesRequest {
                series_id: String::from("test:1"),
                tests: vec![String::from("test1")],
                start_time: Some(prost_types::Timestamp::default()),
                end_time: Some(prost_types::Timestamp::default()),
            })
            .await
            .unwrap()
            .into_inner();

        let mut recv_count = 0;
        while let Some(recv) = stream.next().await {
            assert_eq!(
                // TODO: improve
                recv.unwrap().results.first().unwrap().flag,
                Flag::Inconclusive as i32
            );
            recv_count += 1;
        }
        assert_eq!(recv_count, 6);
    };

    tokio::select! {
        _ = coordinator_future => panic!("coordinator returned first"),
        _ = request_future => (),
    }
}

#[tokio::test]
async fn integration_test_hardcoded_dag() {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::INFO)
    //     .init();

    let data_switch = DataSwitch::new(HashMap::from([(
        "test",
        &TestDataSource as &dyn DataConnector,
    )]));

    let (coordinator_future, mut client) =
        set_up_rove(data_switch, construct_hardcoded_dag()).await;

    let requests_future = async {
        let mut stream = client
            .validate_series(ValidateSeriesRequest {
                series_id: String::from("test:1"),
                tests: vec![String::from("step_check"), String::from("dip_check")],
                start_time: Some(prost_types::Timestamp::default()),
                end_time: Some(prost_types::Timestamp::default()),
            })
            .await
            .unwrap()
            .into_inner();

        let mut step_recv = false;
        let mut dip_recv = false;
        let mut recv_count = 0;
        while let Some(recv) = stream.next().await {
            let inner = recv.unwrap();
            match inner.test.as_ref() {
                "dip_check" => {
                    dip_recv = true;
                }
                "step_check" => {
                    step_recv = true;
                }
                _ => {
                    panic!("unrecognised test name returned")
                }
            }
            let flags: Vec<i32> = inner.results.iter().map(|res| res.flag).collect();
            assert_eq!(flags, vec![Flag::Pass as i32; 1]);
            recv_count += 1;
        }
        assert_eq!(recv_count, 2);
        assert!(dip_recv);
        assert!(step_recv);

        let mut stream = client
            .validate_spatial(ValidateSpatialRequest {
                spatial_id: String::from("test:1"),
                backing_sources: Vec::new(),
                tests: vec!["buddy_check".to_string(), "sct".to_string()],
                time: Some(prost_types::Timestamp::default()),
                polygon: Vec::new(),
            })
            .await
            .unwrap()
            .into_inner();

        let mut buddy_recv = false;
        let mut sct_recv = false;
        let mut recv_count = 0;
        while let Some(recv) = stream.next().await {
            let inner = recv.unwrap();
            match inner.test.as_ref() {
                "buddy_check" => {
                    buddy_recv = true;
                }
                "sct" => {
                    sct_recv = true;
                }
                _ => {
                    panic!("unrecognised test name returned")
                }
            }
            let flags: Vec<i32> = inner.results.iter().map(|res| res.flag).collect();
            assert_eq!(flags, vec![Flag::Pass as i32; DATA_LEN_SPATIAL]);
            recv_count += 1;
        }
        assert_eq!(recv_count, 2);
        assert!(buddy_recv);
        assert!(sct_recv);
    };

    tokio::select! {
        _ = coordinator_future => panic!("coordinator returned first"),
        _ = requests_future => (),
    }
}
