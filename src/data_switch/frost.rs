use crate::{
    data_switch::{duration, TimeseriesCache, Timespec},
    util::Timestamp,
};
use chrono::{prelude::*, Duration};
use chronoutil::RelativeDuration;
use serde::{Deserialize, Deserializer};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
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
    #[error("duration parser failed, invalid duration: {input}")]
    ParseDuration {
        source: duration::Error,
        input: String,
    },
    #[error("{0}")]
    MissingObs(String),
}

#[derive(Deserialize, Debug)]
struct FrostObsBody {
    #[serde(deserialize_with = "des_value")]
    value: f32,
}

// TODO: flatten this with FrostObsBody?
#[derive(Deserialize, Debug)]
struct FrostObs {
    body: FrostObsBody,
    #[serde(deserialize_with = "des_time")]
    time: Timestamp,
}

fn des_value<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
    D: Deserializer<'de>,
    D::Error: serde::de::Error,
{
    use serde::de::Error;
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse().map_err(D::Error::custom)
}

fn des_time<'de, D>(deserializer: D) -> Result<Timestamp, D::Error>
where
    D: Deserializer<'de>,
    D::Error: serde::de::Error,
{
    use serde::de::Error;
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(Timestamp(
        chrono::DateTime::parse_from_rfc3339(s.as_str())
            .map_err(D::Error::custom)?
            .timestamp(),
    ))
}

fn extract_duration(mut metadata_resp: serde_json::Value) -> Result<RelativeDuration, Error> {
    let time_resolution = metadata_resp
        .get_mut("data")
        .ok_or(Error::FindMetadata(
            "couldn't find data field on root".to_string(),
        ))?
        .get_mut("tseries")
        .ok_or(Error::FindMetadata(
            "couldn't find field tseries on data".to_string(),
        ))?
        .get_mut(0)
        .ok_or(Error::FindMetadata("tseries array is empty".to_string()))?
        .get_mut("header")
        .ok_or(Error::FindMetadata(
            "couldn't find field header on 1st tseries".to_string(),
        ))?
        .get_mut("extra")
        .ok_or(Error::FindMetadata(
            "couldn't find field extra on header".to_string(),
        ))?
        .get_mut("timeseries")
        .ok_or(Error::FindMetadata(
            "couldn't find field timeseries on extra".to_string(),
        ))?
        .get_mut("timeresolution")
        .ok_or(Error::FindMetadata(
            "couldn't find field timeresolution on quality".to_string(),
        ))?
        .as_str()
        .ok_or(Error::FindMetadata(
            "field timeresolution was not a string".to_string(),
        ))?;

    duration::parse_duration(time_resolution).map_err(|e| Error::ParseDuration {
        source: e,
        input: time_resolution.to_string(),
    })
}

fn extract_obs(mut resp: serde_json::Value) -> Result<Vec<FrostObs>, Error> {
    let obs_portion = resp
        .get_mut("data")
        .ok_or(Error::FindObs(
            "couldn't find data field on root".to_string(),
        ))?
        .get_mut("tseries")
        .ok_or(Error::FindObs(
            "couldn't find tseries field on data".to_string(),
        ))?
        .get_mut(0)
        .ok_or(Error::FindObs("tseries array is empty".to_string()))?
        .get_mut("observations")
        .ok_or(Error::FindObs(
            "couldn't observations data field on 1st tseries".to_string(),
        ))?
        .take();

    let obs: Vec<FrostObs> = serde_json::from_value(obs_portion)?;

    Ok(obs)
}

pub async fn get_timeseries_data(
    data_id: &str,
    timespec: Timespec,
    num_leading_points: u8,
) -> Result<[f32; 3], Error> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let (station_id, element_id) = data_id
        .split_once('/')
        .ok_or(Error::InvalidDataId(data_id.to_string()))?;

    let (start_time, end_time) = match timespec {
        Timespec::Single(timestamp) => {
            let time = Utc.timestamp_opt(timestamp.0, 0).unwrap();
            (time, time)
        }
        Timespec::Range { start, end } => (
            Utc.timestamp_opt(start.0, 0).unwrap(),
            Utc.timestamp_opt(end.0, 0).unwrap(),
        ),
    };

    let metadata_resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("elementids", element_id),
            ("stationids", station_id),
            ("incobs", "false"),
        ])
        .send()
        .await?
        .json()
        .await?;

    let period = extract_duration(metadata_resp)?;

    let resp: serde_json::Value = client
        .get("https://frost-beta.met.no/api/v1/obs/met.no/filter/get")
        .query(&[
            ("elementids", element_id),
            ("stationids", station_id),
            ("incobs", "true"),
            (
                "time",
                format!(
                    "{}/{}",
                    (start_time - period * i32::from(num_leading_points))
                        .to_rfc3339_opts(SecondsFormat::Secs, true),
                    (end_time + Duration::seconds(1)).to_rfc3339_opts(SecondsFormat::Secs, true)
                )
                .as_str(),
            ),
        ])
        .send()
        .await?
        .json()
        .await?;

    let obs: Vec<FrostObs> = extract_obs(resp)?;

    if obs.len() < num_leading_points as usize + 1 {
        return Err(Error::MissingObs(format!(
            "found {} obs, need at least {}",
            obs.len(),
            num_leading_points + 1,
        )));
    }

    if obs.last().unwrap().time.0 != end_time.timestamp() {
        return Err(Error::MissingObs(
            "final obs timestamp did not match input timestamp".to_string(),
        ));
    }

    Ok(obs
        .into_iter()
        .map(|obs| obs.body.value)
        .take(3)
        .collect::<Vec<f32>>()
        .try_into()
        .unwrap())
}
