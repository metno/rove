use crate::util::Timestamp;
use async_trait::async_trait;
use chronoutil::RelativeDuration;
use olympian::points::Points;
use std::collections::HashMap;
use thiserror::Error;

mod duration;
pub mod frost;

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
pub struct SeriesCache {
    pub start_time: Timestamp,
    pub period: RelativeDuration,
    pub data: Vec<Option<f32>>,
}

pub struct Timerange {
    pub start: Timestamp,
    pub end: Timestamp,
}

#[async_trait]
pub trait DataSource: Sync + std::fmt::Debug {
    async fn get_series_data(
        &self,
        data_id: &str,
        timespec: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, Error>;
    // async fn get_spatial_data(&self, station_id: &str, timestamp: Timestamp);
}

#[derive(Debug)]
pub struct DataSwitch<'ds> {
    sources: HashMap<&'ds str, &'ds dyn DataSource>,
}

impl<'ds> DataSwitch<'ds> {
    pub fn new(sources: HashMap<&'ds str, &'ds dyn DataSource>) -> Self {
        Self { sources }
    }

    pub async fn get_series_data(
        &self,
        series_id: &str,
        timerange: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, Error> {
        // TODO: check these names still make sense
        let (data_source_id, data_id) = series_id
            .split_once(':')
            .ok_or_else(|| Error::InvalidSeriesId(series_id.to_string()))?;

        let data_source = self
            .sources
            .get(data_source_id)
            .ok_or_else(|| Error::InvalidDataSource(data_source_id.to_string()))?;

        data_source
            .get_series_data(data_id, timerange, num_leading_points)
            .await
    }

    pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
        todo!()
    }
}
