use coordinator_pb::{coordinator_client::CoordinatorClient, ValidateOneRequest};
use prost_types::Timestamp;
use rove::{coordinator, runner, util::ListenerType};
use std::sync::Arc;
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

#[tokio::test]
async fn integration_test() {
    let runner_socket = NamedTempFile::new().unwrap();
    let runner_socket = Arc::new(runner_socket.into_temp_path());
    std::fs::remove_file(&*runner_socket).unwrap();
    let runner_uds = UnixListener::bind(&*runner_socket).unwrap();
    let runner_stream = UnixListenerStream::new(runner_uds);
    let runner_future = async {
        runner::start_server(ListenerType::UnixListener(runner_stream))
            .await
            .unwrap();
    };

    let coordintor_socket = NamedTempFile::new().unwrap();
    let coordintor_socket = Arc::new(coordintor_socket.into_temp_path());
    std::fs::remove_file(&*coordintor_socket).unwrap();
    let coordintor_uds = UnixListener::bind(&*coordintor_socket).unwrap();
    let coordintor_stream = UnixListenerStream::new(coordintor_uds);
    let coordinator_future = async {
        coordinator::start_server(
            ListenerType::UnixListener(coordintor_stream),
            coordinator::EndpointType::Socket(Arc::clone(&runner_socket)),
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
        _ = runner_future => panic!("runner returned first"),
        _ = coordinator_future => panic!("coordinator returned first"),
        _ = request_future => (),
    }
}
