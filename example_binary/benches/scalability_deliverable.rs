use async_trait::async_trait;
use chronoutil::RelativeDuration;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use example_binary::construct_hardcoded_dag;
use rove::{
    data_switch,
    data_switch::{DataSource, DataSwitch, SeriesCache, SpatialCache, Timerange, Timestamp},
    pb::{rove_client::RoveClient, ValidateSeriesRequest},
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

const TEST_PARALLELISM: u64 = 100;

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
            data: vec![Some(1.), Some(1.), Some(1.)],
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

fn spawn_server(runtime: &Runtime) -> (Channel, JoinHandle<()>) {
    let (channel, server_future) = runtime.block_on(async {
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

async fn spam_series(channel: Channel) {
    // TODO: this client is redundant?
    let client = RoveClient::new(channel);

    let mut resps = JoinSet::new();

    for _ in 0..TEST_PARALLELISM {
        let mut client = client.clone();
        let req = ValidateSeriesRequest {
            series_id: String::from("test:1"),
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

pub fn series_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    let (channel, _server_handle) = spawn_server(&runtime);

    let mut group = c.benchmark_group("series_benchmark");
    // TODO: reconsider these params?
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.throughput(Throughput::Elements(TEST_PARALLELISM));
    group.bench_with_input(
        BenchmarkId::new("series_benchmark", 1),
        &channel,
        |b, chan| b.to_async(&runtime).iter(|| spam_series(chan.clone())),
    );
    group.finish();
}

criterion_group!(benches, series_benchmark);
criterion_main!(benches);
