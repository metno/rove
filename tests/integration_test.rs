use async_trait::async_trait;
use coordinator_pb::{coordinator_client::CoordinatorClient, ValidateOneRequest};
use prost_types::Timestamp;
use rove::{
    coordinator, data_switch,
    data_switch::{DataSource, DataSwitch, SeriesCache, Timespec},
    util::ListenerType,
};
use std::{collections::HashMap, sync::Arc};
use tempfile::NamedTempFile;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::{wrappers::UnixListenerStream, StreamExt};
use tonic::transport::Endpoint;
use tower::service_fn;

mod util {
    tonic::include_proto!("util");
}

mod coordinator_pb {
    tonic::include_proto!("coordinator");
}

#[derive(Debug)]
struct TestDataSource;

#[async_trait]
impl DataSource for TestDataSource {
    async fn get_series_data(
        &self,
        _data_id: &str,
        _timespec: Timespec,
        _num_leading_points: u8,
    ) -> Result<SeriesCache, data_switch::Error> {
        Ok(SeriesCache(Vec::new()))
    }
}

#[tokio::test]
async fn integration_test() {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::INFO)
    //     .init();

    let data_switch = DataSwitch::new(HashMap::from([(
        "test",
        &TestDataSource as &dyn DataSource,
    )]));

    let coordintor_socket = NamedTempFile::new().unwrap();
    let coordintor_socket = Arc::new(coordintor_socket.into_temp_path());
    std::fs::remove_file(&*coordintor_socket).unwrap();
    let coordintor_uds = UnixListener::bind(&*coordintor_socket).unwrap();
    let coordintor_stream = UnixListenerStream::new(coordintor_uds);
    let coordinator_future = async {
        coordinator::start_server(ListenerType::UnixListener(coordintor_stream), data_switch)
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
    let mut client = CoordinatorClient::new(coordinator_channel);

    let request_future = async {
        let mut stream = client
            .validate_one(ValidateOneRequest {
                series_id: String::from("test:1"),
                tests: vec![String::from("test1")],
                time: Some(Timestamp::default()),
            })
            .await
            .unwrap()
            .into_inner();

        let mut recv_count = 0;
        while let Some(recv) = stream.next().await {
            assert_eq!(recv.unwrap().flag, util::Flag::Inconclusive as i32);
            recv_count += 1;
        }
        assert_eq!(recv_count, 6);
    };

    tokio::select! {
        _ = coordinator_future => panic!("coordinator returned first"),
        _ = request_future => (),
    }
}
