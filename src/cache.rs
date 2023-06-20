use olympian::points::Points;
use serde::{de::Error, Deserialize, Deserializer};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum CacheError {
    #[error("series id `{0}` could not be parsed")]
    InvalidSeriesId(String),
    #[error("data source `{0}` not registered")]
    InvalidDataSource(String),
    #[error("frost connector failed")]
    Frost(#[from] FrostError),
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum FrostError {
    #[error("fetching data from frost failed")]
    Request(#[from] reqwest::Error),
    #[error("failed to find obs in json body: {0}")]
    FindObs(String),
    #[error("failed to deserialise obs to struct")]
    DeserializeObs(#[from] serde_json::Error),
}

#[derive(Deserialize, Debug)]
struct FrostObsBody {
    #[serde(deserialize_with = "des_value")]
    value: f32,
}

#[derive(Deserialize, Debug)]
struct FrostObs {
    body: FrostObsBody,
    time: String,
}

fn des_value<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
    D: Deserializer<'de>,
    D::Error: serde::de::Error,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse().map_err(D::Error::custom)
}

pub async fn get_timeseries_data(
    series_id: String,
    unix_timestamp: i64,
) -> Result<[f32; 3], CacheError> {
    let (data_source, data_id) = series_id
        .split_once(':')
        .ok_or(CacheError::InvalidSeriesId(series_id.clone()))?;

    // TODO: find a more flexible and elegant way of handling this
    match data_source {
        "frost" => get_timeseries_data_frost(data_id, unix_timestamp)
            .await
            .map_err(CacheError::Frost),
        _ => Err(CacheError::InvalidDataSource(data_source.to_string())),
    }
}

pub async fn get_timeseries_data_frost(
    _data_id: &str,
    _unix_timestamp: i64,
) -> Result<[f32; 3], FrostError> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let mut resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("elementids", "air_temperature"),
            ("stationids", "18700"),
            ("incobs", "true"),
            ("time", "latest"),
        ])
        .send()
        .await?
        .json()
        .await?;

    let obs_portion = resp
        .get_mut("data")
        .ok_or(FrostError::FindObs(
            "couldn't find data field on root".to_string(),
        ))?
        .get_mut("tseries")
        .ok_or(FrostError::FindObs(
            "couldn't find tseries field on data".to_string(),
        ))?
        .get_mut(0)
        .ok_or(FrostError::FindObs("tseries array is empty".to_string()))?
        .get_mut("observations")
        .ok_or(FrostError::FindObs(
            "couldn't observations data field on 1st tseries".to_string(),
        ))?
        .take();

    let obs: Vec<FrostObs> = serde_json::from_value(obs_portion)?;

    println!(
        "{:?}",
        obs.into_iter()
            .map(|obs| (obs.body.value, obs.time))
            .collect::<Vec<(f32, String)>>()
    );

    Ok([1., 1., 1.]) // TODO get actual data
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
