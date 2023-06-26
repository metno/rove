use olympian::points::Points;
use thiserror::Error;

mod duration_parser;
mod frost;

#[derive(Error, Debug)]
#[non_exhaustive]
// TODO: should we rename these to just Error since they're already scoped?
pub enum CacheError {
    #[error("series id `{0}` could not be parsed")]
    InvalidSeriesId(String),
    #[error("data source `{0}` not registered")]
    InvalidDataSource(String),
    #[error("frost connector failed")]
    Frost(#[from] frost::FrostError),
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
        "frost" => frost::get_timeseries_data(data_id, unix_timestamp)
            .await
            .map_err(CacheError::Frost),
        _ => Err(CacheError::InvalidDataSource(data_source.to_string())),
    }
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
