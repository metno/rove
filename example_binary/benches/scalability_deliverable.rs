use async_trait::async_trait;
use chronoutil::RelativeDuration;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use rove::{
    data_switch,
    data_switch::{DataSource, DataSwitch, SeriesCache, SpatialCache, Timerange, Timestamp},
    pb::{rove_client::RoveClient, ValidateSeriesRequest},
    server::{start_server, ListenerType},
};
use std::{
    collections::HashMap,
    sync::Arc,
    thread::{self, JoinHandle},
};
use tempfile::NamedTempFile;
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Runtime;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

#[derive(Debug)]
struct BenchDataSource;

#[async_trait]
impl DataSource for BenchDataSource {
    async fn get_series_data(
        &self,
        _data_id: &str,
        _timespec: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, data_switch::Error> {
        Ok(SeriesCache {
            start_time: Timestamp(0),
            period: RelativeDuration::minutes(5),
            data: Vec::new(),
            num_leading_points,
        })
    }

    async fn get_spatial_data(
        &self,
        _timestamp: Timestamp,
    ) -> Result<SpatialCache, data_switch::Error> {
        unimplemented!()
    }
}

fn spawn_server() -> (Channel, JoinHandle<()>) {
    let server_runtime = Runtime::new().unwrap();

    let (channel, server_future) = server_runtime.block_on(async {
        let data_switch = DataSwitch::new(HashMap::from([(
            "test",
            &BenchDataSource as &dyn DataSource,
        )]));

        let coordintor_socket = NamedTempFile::new().unwrap();
        let coordintor_socket = Arc::new(coordintor_socket.into_temp_path());
        std::fs::remove_file(&*coordintor_socket).unwrap();
        let coordintor_uds = UnixListener::bind(&*coordintor_socket).unwrap();
        let coordintor_stream = UnixListenerStream::new(coordintor_uds);
        let coordinator_future = async {
            start_server(ListenerType::UnixListener(coordintor_stream), data_switch)
                .await
                .unwrap();
        };

        (
            Endpoint::try_from("http://any.url")
                .unwrap()
                .connect_with_connector(service_fn(move |_: tonic::transport::Uri| {
                    let socket = Arc::clone(&coordintor_socket);
                    async move { UnixStream::connect(&*socket).await }
                }))
                .await
                .unwrap(),
            coordinator_future,
        )
    });

    let join_handle = thread::spawn(move || server_runtime.block_on(server_future));

    (channel, join_handle)

    // tokio::select! {
    //     _ = coordinator_future => panic!("coordinator returned first"),
    //     _ = request_future => (),
    // }
}

async fn spam_series(channel: Channel) {
    // let request_future = async {
    //     let mut stream = client
    //         .validate_series(ValidateSeriesRequest {
    //             series_id: String::from("test:1"),
    //             tests: vec![String::from("test1")],
    //             start_time: Some(prost_types::Timestamp::default()),
    //             end_time: Some(prost_types::Timestamp::default()),
    //         })
    //         .await
    //         .unwrap()
    //         .into_inner();

    //     let mut recv_count = 0;
    //     while let Some(recv) = stream.next().await {
    //         assert_eq!(
    //             // TODO: improve
    //             recv.unwrap().results.first().unwrap().flag,
    //             Flag::Inconclusive as i32
    //         );
    //         recv_count += 1;
    //     }
    //     assert_eq!(recv_count, 6);
    // };

    // let req = ValidateSeriesRequest {
    //     series_id: "frost:18700/air_temperature".to_string(),
    //     start_time: Some(prost_types::Timestamp {
    //         seconds: Utc
    //             .with_ymd_and_hms(2021, 6, 26, 12, 0, 0)
    //             .unwrap()
    //             .timestamp(),
    //         nanos: 0,
    //     }),
    //     end_time: Some(prost_types::Timestamp {
    //         seconds: Utc
    //             .with_ymd_and_hms(2023, 6, 26, 14, 0, 0)
    //             .unwrap()
    //             .timestamp(),
    //         nanos: 0,
    //     }),
    //     tests: vec!["step_check".to_string(), "dip_check".to_string()],
    // };

    let req = ValidateSeriesRequest {
        series_id: String::from("test:1"),
        tests: vec![String::from("test1")],
        start_time: Some(prost_types::Timestamp::default()),
        end_time: Some(prost_types::Timestamp::default()),
    };

    // let mut client = RoveClient::connect("http://[::1]:1337").await.unwrap();
    let mut client = RoveClient::new(channel);

    client.validate_series(req).await.unwrap();
}

pub fn series_benchmark(c: &mut Criterion) {
    let (channel, _server_handle) = spawn_server();

    let mut group = c.benchmark_group("series_benchmark");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("series_benchmark", 1),
        &channel,
        |b, chan| {
            // TODO: figure out how to reuse runtime?
            b.to_async(Runtime::new().unwrap())
                .iter(|| spam_series(chan.clone()))
        },
    );
    group.finish();
}

criterion_group!(benches, series_benchmark);
criterion_main!(benches);
