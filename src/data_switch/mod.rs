use crate::util::Timestamp;
use async_trait::async_trait;
use olympian::points::Points;
use thiserror::Error;

mod duration;
mod frost;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("series id `{0}` could not be parsed")]
    InvalidSeriesId(String),
    #[error("data source `{0}` not registered")]
    InvalidDataSource(String),
    // TODO: remove this and provide proper errors to map to
    #[error("frost connector failed")]
    Frost(#[from] frost::Error),
}

// TODO: move this to olympian?
pub struct SeriesCache(pub Vec<(Timestamp, f32)>);

pub enum Timespec {
    Single(Timestamp),
    Range { start: Timestamp, end: Timestamp },
}

#[async_trait]
pub trait DataSource {
    async fn get_series_data(
        &self,
        data_id: &str,
        timespec: Timespec,
        num_leading_points: u8,
    ) -> Result<SeriesCache, Error>;
    // async fn get_spatial_data(&self, station_id: &str, timestamp: Timestamp);
}

pub async fn get_series_data(
    series_id: &str,
    timespec: Timespec,
    num_leading_points: u8,
) -> Result<SeriesCache, Error> {
    let (data_source, data_id) = series_id
        .split_once(':')
        .ok_or_else(|| Error::InvalidSeriesId(series_id.to_string()))?;

    let frost_src = frost::Frost;

    // TODO: find a more flexible and elegant way of handling this
    match data_source {
        "frost" => {
            frost_src
                .get_series_data(data_id, timespec, num_leading_points)
                .await
        }
        "test" => Ok(SeriesCache(Vec::new())),
        _ => Err(Error::InvalidDataSource(data_source.to_string())),
    }
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
