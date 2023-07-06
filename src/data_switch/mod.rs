use crate::util::Timestamp;
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
    #[error("frost connector failed")]
    Frost(#[from] frost::Error),
}

pub enum Timespec {
    Single(Timestamp),
    Range { start: Timestamp, end: Timestamp },
}

pub async fn get_timeseries_data(
    series_id: String,
    timespec: Timespec,
    num_leading_points: u8,
) -> Result<[f32; 3], Error> {
    let (data_source, data_id) = series_id
        .split_once(':')
        .ok_or(Error::InvalidSeriesId(series_id.clone()))?;

    // TODO: find a more flexible and elegant way of handling this
    match data_source {
        "frost" => frost::get_timeseries_data(data_id, timespec, num_leading_points)
            .await
            .map_err(Error::Frost),
        _ => Err(Error::InvalidDataSource(data_source.to_string())),
    }
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
