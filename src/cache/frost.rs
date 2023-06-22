use chrono::prelude::*;
use serde::{de::Error, Deserialize, Deserializer};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
// TODO: should we rename these to just Error since they're already scoped?
pub enum FrostError {
    #[error("data id `{0}` could not be parsed")]
    InvalidDataId(String),
    #[error("fetching data from frost failed")]
    Request(#[from] reqwest::Error),
    #[error("failed to find obs in json body: {0}")]
    FindObs(String),
    #[error("failed to deserialise obs to struct")]
    DeserializeObs(#[from] serde_json::Error),
    #[error("failed to find metadata in json body: {0}")]
    FindMetadata(String),
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
    data_id: &str,
    unix_timestamp: i64,
) -> Result<[f32; 3], FrostError> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let (station_id, element_id) = data_id
        .split_once('/')
        .ok_or(FrostError::InvalidDataId(data_id.to_string()))?;

    let time = Utc.timestamp_opt(unix_timestamp, 0).unwrap();

    let mut metadata_resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("elementids", element_id),
            ("stationids", station_id),
            ("incobs", "false"),
        ])
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let time_resolution = metadata_resp
        .get_mut("data")
        .ok_or(FrostError::FindMetadata(
            "couldn't find data field on root".to_string(),
        ))?
        .get_mut("tseries")
        .ok_or(FrostError::FindMetadata(
            "couldn't find field tseries on data".to_string(),
        ))?
        .get_mut(0)
        .ok_or(FrostError::FindMetadata(
            "tseries array is empty".to_string(),
        ))?
        .get_mut("header")
        .ok_or(FrostError::FindMetadata(
            "couldn't find field header on 1st tseries".to_string(),
        ))?
        .get_mut("extra")
        .ok_or(FrostError::FindMetadata(
            "couldn't find field extra on header".to_string(),
        ))?
        .get_mut("timeseries")
        .ok_or(FrostError::FindMetadata(
            "couldn't find field timeseries on extra".to_string(),
        ))?
        .get_mut("timeresolution")
        .ok_or(FrostError::FindMetadata(
            "couldn't find field timeresolution on quality".to_string(),
        ))?
        .as_str()
        .ok_or(FrostError::FindMetadata(
            "field timeresolution was not a string".to_string(),
        ))?;

    println!("{}", time_resolution);

    let mut resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("elementids", element_id),
            ("stationids", station_id),
            ("incobs", "true"),
            (
                "time",
                time.to_rfc3339_opts(SecondsFormat::Secs, true).as_str(),
            ),
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
