use core::future::Future;
use pb::{rove_client::RoveClient, validate_request::SpaceSpec, Flag, ValidateRequest};
use rove::{
    data_switch::{DataConnector, DataSwitch},
    dev_utils::{construct_hardcoded_pipeline, TestDataSource},
    start_server_unix_listener, Pipeline,
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

pub async fn set_up_rove(
    data_switch: DataSwitch<'static>,
    pipelines: HashMap<String, Pipeline>,
) -> (impl Future<Output = ()>, RoveClient<Channel>) {
    let coordintor_socket = NamedTempFile::new().unwrap();
    let coordintor_socket = Arc::new(coordintor_socket.into_temp_path());
    std::fs::remove_file(&*coordintor_socket).unwrap();
    let coordintor_uds = UnixListener::bind(&*coordintor_socket).unwrap();
    let coordintor_stream = UnixListenerStream::new(coordintor_uds);
    let coordinator_future = async {
        start_server_unix_listener(coordintor_stream, data_switch, pipelines)
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

// TODO: we should probably just use one of the sample pipelines here once we have the checks
// from olympian working
#[tokio::test]
async fn integration_test_hardcoded_pipeline() {
    let data_switch = DataSwitch::new(HashMap::from([(
        "test",
        &TestDataSource {
            data_len_single: DATA_LEN_SINGLE,
            data_len_series: 1,
            data_len_spatial: DATA_LEN_SPATIAL,
        } as &dyn DataConnector,
    )]));

    let (coordinator_future, mut client) =
        set_up_rove(data_switch, construct_hardcoded_pipeline()).await;

    let requests_future = async {
        let mut stream = client
            .validate(ValidateRequest {
                data_source: String::from("test"),
                backing_sources: vec![],
                start_time: Some(prost_types::Timestamp::default()),
                end_time: Some(prost_types::Timestamp::default()),
                time_resolution: String::from("PT5M"),
                space_spec: Some(SpaceSpec::All(())),
                pipeline: String::from("hardcoded"),
                extra_spec: None,
            })
            .await
            .unwrap()
            .into_inner();

        let mut step_recv_count = 0;
        let mut spike_recv_count = 0;
        let mut buddy_recv_count = 0;
        let mut sct_recv_count = 0;
        while let Some(recv) = stream.next().await {
            let inner = recv.unwrap();
            match inner.test.as_ref() {
                "spike_check" => {
                    spike_recv_count += 1;
                }
                "step_check" => {
                    step_recv_count += 1;
                }
                "buddy_check" => {
                    buddy_recv_count += 1;
                }
                "sct" => {
                    sct_recv_count += 1;
                }
                _ => {
                    panic!("unrecognised test name returned")
                }
            }
            let flags: Vec<i32> = inner.results.iter().map(|res| res.flag).collect();
            assert!(
                flags == vec![Flag::Pass as i32; DATA_LEN_SPATIAL]
                    || flags == vec![Flag::Isolated as i32; DATA_LEN_SPATIAL]
            );
        }
        assert_eq!(spike_recv_count, 1);
        assert_eq!(step_recv_count, 1);
        assert_eq!(buddy_recv_count, 1);
        assert_eq!(sct_recv_count, 1);
    };

    tokio::select! {
        _ = coordinator_future => panic!("coordinator returned first"),
        _ = requests_future => (),
    }
}
