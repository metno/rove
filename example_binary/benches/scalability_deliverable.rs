use chrono::prelude::*;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use prost_types::Timestamp;
use rove::pb::{rove_client::RoveClient, ValidateSeriesRequest};
use tokio::runtime::Runtime;
// use tonic::transport::Channel;

async fn spam_series() {
    let req = ValidateSeriesRequest {
        series_id: "frost:18700/air_temperature".to_string(),
        start_time: Some(Timestamp {
            seconds: Utc
                .with_ymd_and_hms(2021, 6, 26, 12, 0, 0)
                .unwrap()
                .timestamp(),
            nanos: 0,
        }),
        end_time: Some(Timestamp {
            seconds: Utc
                .with_ymd_and_hms(2023, 6, 26, 14, 0, 0)
                .unwrap()
                .timestamp(),
            nanos: 0,
        }),
        tests: vec!["step_check".to_string(), "dip_check".to_string()],
    };

    let mut client = RoveClient::connect("http://[::1]:1337").await.unwrap();

    client.validate_series(req).await.unwrap();
}

pub fn series_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("series_benchmark");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.bench_function("series_benchmark", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| spam_series())
    });
    group.finish();
}

criterion_group!(benches, series_benchmark);
criterion_main!(benches);
