use crate::{
    data_switch,
    data_switch::{duration, DataSource, SeriesCache, Timerange},
    util::Timestamp,
};
use async_trait::async_trait;
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
    #[error("{0}")]
    Misalignment(String),
}

#[derive(Debug)]
pub struct Frost;

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
    time: DateTime<Utc>,
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

fn des_time<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
    D::Error: serde::de::Error,
{
    use serde::de::Error;
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(chrono::DateTime::parse_from_rfc3339(s.as_str())
        .map_err(D::Error::custom)?
        .with_timezone(&Utc))
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

async fn get_series_data_inner(
    data_id: &str,
    timespec: Timerange,
    num_leading_points: u8,
) -> Result<SeriesCache, Error> {
    // TODO: figure out how to share the client between rove reqs
    let client = reqwest::Client::new();

    let (station_id, element_id) = data_id
        .split_once('/')
        .ok_or(Error::InvalidDataId(data_id.to_string()))?;

    let (interval_start, interval_end) = match timespec {
        Timerange::Single(timestamp) => {
            let time = Utc.timestamp_opt(timestamp.0, 0).unwrap();
            (time, time)
        }
        Timerange::Range { start, end } => (
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
                    (interval_start - period * i32::from(num_leading_points))
                        .to_rfc3339_opts(SecondsFormat::Secs, true),
                    (interval_end + Duration::seconds(1))
                        .to_rfc3339_opts(SecondsFormat::Secs, true)
                )
                .as_str(),
            ),
        ])
        .send()
        .await?
        .json()
        .await?;

    let obses: Vec<FrostObs> = extract_obs(resp)?;

    // TODO: send this part to rayon?

    // TODO: preallocate?
    // let ts_length = (end_time - first_obs_time) / period;
    let mut data = Vec::new();

    let mut curr_obs_time = interval_start - period * i32::from(num_leading_points);
    let first_obs_time = obses
        .first()
        .ok_or(Error::MissingObs(
            "obs array from frost is empty".to_string(),
        ))?
        .time;

    // handle misalignment of interval_start with ts, and leading missing values
    if curr_obs_time != first_obs_time {
        if first_obs_time < curr_obs_time {
            return Err(Error::Misalignment(
                "the first obs returned by frost is outside the time range".to_string(),
            ));
        }

        while first_obs_time >= curr_obs_time + period {
            data.push(None);
            curr_obs_time = curr_obs_time + period;
        }

        curr_obs_time = first_obs_time;
    }

    let num_preceding_nones = i32::try_from(data.len()).map_err(|_| {
        Error::MissingObs("i32 overflow: too many missing obs at start".to_string())
    })?;
    let start_time = Timestamp((first_obs_time - period * num_preceding_nones).timestamp());

    // insert obses into data, with Nones for gaps in the series
    for obs in obses {
        if curr_obs_time == obs.time {
            data.push(Some(obs.body.value));
            curr_obs_time = curr_obs_time + period;
        } else {
            while curr_obs_time < obs.time {
                data.push(None);
                curr_obs_time = curr_obs_time + period;
            }
            if curr_obs_time == obs.time {
                data.push(Some(obs.body.value));
                curr_obs_time = curr_obs_time + period;
            } else {
                return Err(Error::Misalignment(
                    "obs misaligned with series".to_string(),
                ));
            }
        }
    }

    // handle trailing missing values
    while curr_obs_time < interval_end {
        data.push(None);
        curr_obs_time = curr_obs_time + period;
    }

    Ok(SeriesCache {
        start_time,
        period,
        data,
    })
}

#[async_trait]
impl DataSource for Frost {
    async fn get_series_data(
        &self,
        data_id: &str,
        timespec: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, data_switch::Error> {
        get_series_data_inner(data_id, timespec, num_leading_points)
            .await
            .map_err(data_switch::Error::Frost)
    }
}
