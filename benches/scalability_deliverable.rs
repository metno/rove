use async_trait::async_trait;
use chronoutil::RelativeDuration;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use dagmar::Dag;
use pb::{rove_client::RoveClient, ValidateSeriesRequest, ValidateSpatialRequest};
use rove::{
    data_switch::{
        self, DataConnector, DataSwitch, GeoPoint, SeriesCache, SpatialCache, Timerange, Timestamp,
    },
    server::{start_server, ListenerType},
};
use std::{collections::HashMap, sync::Arc};
use tempfile::NamedTempFile;
use tokio::{
    net::{UnixListener, UnixStream},
    runtime::Runtime,
    task::{JoinHandle, JoinSet},
};
use tokio_stream::{wrappers::UnixListenerStream, StreamExt};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

mod pb {
    tonic::include_proto!("rove");
}

const TEST_PARALLELISM_SINGLE: u64 = 100;
const TEST_PARALLELISM_SERIES: u64 = 10;
const TEST_PARALLELISM_SPATIAL: u64 = 10;
const DATA_LEN_SINGLE: usize = 3;
const DATA_LEN_SERIES: usize = 10000;
const DATA_LEN_SPATIAL: usize = 1000;

#[derive(Debug)]
struct BenchDataSource;

#[async_trait]
impl DataConnector for BenchDataSource {
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
                data: vec![Some(1.); DATA_LEN_SINGLE],
                num_leading_points,
            })),
            "series" => black_box(Ok(SeriesCache {
                start_time: Timestamp(0),
                period: RelativeDuration::minutes(5),
                data: vec![Some(1.); DATA_LEN_SERIES],
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
            (0..DATA_LEN_SPATIAL)
                .map(|i| ((i as f32).powi(2) * 0.001) % 3.)
                .collect(),
            (0..DATA_LEN_SPATIAL)
                .map(|i| ((i as f32 + 1.).powi(2) * 0.001) % 3.)
                .collect(),
            vec![1.; DATA_LEN_SPATIAL],
            vec![1.; DATA_LEN_SPATIAL],
        )))
    }
}

pub fn construct_hardcoded_dag() -> Dag<String> {
    let mut dag: Dag<String> = Dag::new();

    dag.add_node(String::from("dip_check"));
    dag.add_node(String::from("step_check"));
    dag.add_node(String::from("buddy_check"));
    dag.add_node(String::from("sct"));

    dag
}

fn spawn_server(runtime: &Runtime) -> (Channel, JoinHandle<()>) {
    let (channel, server_future) = runtime.block_on(async {
        let data_switch = DataSwitch::new(HashMap::from([(
            "bench",
            &BenchDataSource as &dyn DataConnector,
        )]));

        let coordintor_socket = NamedTempFile::new().unwrap();
        let coordintor_socket = Arc::new(coordintor_socket.into_temp_path());
        std::fs::remove_file(&*coordintor_socket).unwrap();
        let coordintor_uds = UnixListener::bind(&*coordintor_socket).unwrap();
        let coordintor_stream = UnixListenerStream::new(coordintor_uds);
        let coordinator_future = async {
            start_server(
                ListenerType::UnixListener(coordintor_stream),
                data_switch,
                construct_hardcoded_dag(),
            )
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

    // TODO: would there be a point in selecting against this in the benches?
    let join_handle = runtime.spawn(server_future);

    (channel, join_handle)
}

async fn spam_single(channel: Channel) {
    // TODO: this client is redundant?
    let client = RoveClient::new(channel);

    let mut resps = JoinSet::new();

    for _ in 0..TEST_PARALLELISM_SINGLE {
        let mut client = client.clone();
        let req = ValidateSeriesRequest {
            series_id: String::from("bench:single"),
            tests: vec!["step_check".to_string(), "dip_check".to_string()],
            start_time: Some(prost_types::Timestamp::default()),
            end_time: Some(prost_types::Timestamp::default()),
        };

        resps.spawn(tokio::spawn(
            async move { client.validate_series(req).await },
        ));
    }

    while let Some(resp) = resps.join_next().await {
        let mut stream = resp.unwrap().unwrap().unwrap().into_inner();

        let mut recv_count = 0;
        while let Some(_recv) = stream.next().await {
            // assert_eq!(
            //     // TODO: improve
            //     recv.unwrap().results.first().unwrap().flag,
            //     Flag::Inconclusive as i32
            // );
            recv_count += 1;
        }
        assert_eq!(recv_count, 2);
    }
}

async fn spam_series(channel: Channel) {
    // TODO: this client is redundant?
    let client = RoveClient::new(channel);

    let mut resps = JoinSet::new();

    for _ in 0..TEST_PARALLELISM_SERIES {
        let mut client = client.clone();
        let req = ValidateSeriesRequest {
            series_id: String::from("bench:series"),
            tests: vec!["step_check".to_string(), "dip_check".to_string()],
            start_time: Some(prost_types::Timestamp::default()),
            end_time: Some(prost_types::Timestamp::default()),
        };

        resps.spawn(tokio::spawn(
            async move { client.validate_series(req).await },
        ));
    }

    while let Some(resp) = resps.join_next().await {
        let mut stream = resp.unwrap().unwrap().unwrap().into_inner();

        let mut recv_count = 0;
        while let Some(_recv) = stream.next().await {
            // assert_eq!(
            //     // TODO: improve
            //     recv.unwrap().results.first().unwrap().flag,
            //     Flag::Inconclusive as i32
            // );
            recv_count += 1;
        }
        assert_eq!(recv_count, 2);
    }
}

async fn spam_spatial(channel: Channel) {
    // TODO: this client is redundant?
    let client = RoveClient::new(channel);

    let mut resps = JoinSet::new();

    for _ in 0..TEST_PARALLELISM_SPATIAL {
        let mut client = client.clone();
        let req = ValidateSpatialRequest {
            spatial_id: String::from("bench:spatial"),
            backing_sources: Vec::new(),
            tests: vec!["buddy_check".to_string(), "sct".to_string()],
            time: Some(prost_types::Timestamp::default()),
            polygon: Vec::new(),
        };

        resps.spawn(tokio::spawn(
            async move { client.validate_spatial(req).await },
        ));
    }

    while let Some(resp) = resps.join_next().await {
        let mut stream = resp.unwrap().unwrap().unwrap().into_inner();

        let mut recv_count = 0;
        while let Some(_recv) = stream.next().await {
            // assert_eq!(
            //     // TODO: improve
            //     recv.unwrap().results.first().unwrap().flag,
            //     Flag::Inconclusive as i32
            // );
            recv_count += 1;
        }
        assert_eq!(recv_count, 2);
    }
}

pub fn single_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    let (channel, _server_handle) = spawn_server(&runtime);

    let mut group = c.benchmark_group("single_benchmark");
    // TODO: reconsider these params?
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.throughput(Throughput::Elements(TEST_PARALLELISM_SINGLE));
    group.bench_with_input(
        BenchmarkId::new("single_benchmark", TEST_PARALLELISM_SINGLE),
        &channel,
        |b, chan| b.to_async(&runtime).iter(|| spam_single(chan.clone())),
    );
    group.finish();
}

pub fn series_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    let (channel, _server_handle) = spawn_server(&runtime);

    let mut group = c.benchmark_group("series_benchmark");
    // TODO: reconsider these params?
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.throughput(Throughput::Elements(
        TEST_PARALLELISM_SERIES * DATA_LEN_SERIES as u64,
    ));
    group.bench_with_input(
        BenchmarkId::new("series_benchmark", TEST_PARALLELISM_SERIES),
        &channel,
        |b, chan| b.to_async(&runtime).iter(|| spam_series(chan.clone())),
    );
    group.finish();
}

pub fn spatial_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    let (channel, _server_handle) = spawn_server(&runtime);

    let mut group = c.benchmark_group("spatial_benchmark");
    // TODO: reconsider these params?
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.throughput(Throughput::Elements(
        TEST_PARALLELISM_SPATIAL * DATA_LEN_SPATIAL as u64,
    ));
    group.bench_with_input(
        BenchmarkId::new("spatial_benchmark", TEST_PARALLELISM_SPATIAL),
        &channel,
        |b, chan| b.to_async(&runtime).iter(|| spam_spatial(chan.clone())),
    );
    group.finish();
}

criterion_group!(
    benches,
    single_benchmark,
    series_benchmark,
    spatial_benchmark
);
criterion_main!(benches);
