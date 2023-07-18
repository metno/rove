use async_trait::async_trait;
use chrono::prelude::*;
use rove::{
    data_switch,
    data_switch::{DataSource, SeriesCache, SpatialCache, Timerange},
    util::Timestamp,
};
use serde::{Deserialize, Deserializer};
use thiserror::Error;

// TODO: move duration into series?
mod duration;
mod series;

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

#[async_trait]
impl DataSource for Frost {
    async fn get_series_data(
        &self,
        data_id: &str,
        timerange: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, data_switch::Error> {
        series::get_series_data_inner(data_id, timerange, num_leading_points)
            .await
            .map_err(|e| data_switch::Error::CatchAll(format!("{}", e)))
    }

    async fn get_spatial_data(
        &self,
        _source_id: &str,
        _timestamp: Timestamp,
    ) -> Result<SpatialCache, data_switch::Error> {
        todo!()
    }
}
