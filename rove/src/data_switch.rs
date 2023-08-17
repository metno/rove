use async_trait::async_trait;
use chronoutil::RelativeDuration;
use olympian::points::{CoordinateType, Points};
use std::collections::HashMap;
use thiserror::Error;
use crate::pb::util::GeoPoint;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("series id `{0}` could not be parsed")]
    InvalidSeriesId(String),
    #[error("data source `{0}` not registered")]
    InvalidDataSource(String),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("this data source does not offer series data: {0}")]
    SeriesUnimplemented(String),
    #[error("this data source does not offer spatial data: {0}")]
    SpatialUnimplemented(String),
    #[error("tokio task failure")]
    JoinError(#[from] tokio::task::JoinError),
    // TODO: remove this and provide proper errors to map to
    #[error("connector failed: {0}")]
    CatchAll(String),
}

/// Unix timestamp, inner i64 is seconds since unix epoch
#[derive(Debug)]
pub struct Timestamp(pub i64);

pub struct Timerange {
    pub start: Timestamp,
    pub end: Timestamp,
}

//pub struct Polygon(Vec<GeoPoint>);

// TODO: move this to olympian?
pub struct SeriesCache {
    pub start_time: Timestamp,
    pub period: RelativeDuration,
    pub data: Vec<Option<f32>>,
    pub num_leading_points: u8,
}

pub struct SpatialCache {
    pub rtree: Points,
    pub data: Vec<f32>,
}

impl SpatialCache {
    pub fn new(lats: Vec<f32>, lons: Vec<f32>, elevs: Vec<f32>, values: Vec<f32>) -> Self {
        // TODO: ensure vecs have same size
        // TODO: figure out what to do about lafs and ctype
        let n = values.len();
        Self {
            rtree: Points::from_latlons(lats, lons, elevs, vec![0.; n], CoordinateType::Cartesian),
            data: values,
        }
    }
}

#[async_trait]
pub trait DataSource: Sync + std::fmt::Debug {
    async fn get_series_data(
        &self,
        data_id: &str,
        timespec: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, Error>;

    // TODO: add a str param for extra specification?
    async fn get_spatial_data(
        &self,
        polygon: Vec<GeoPoint>,
        extra_spec: &str, 
        timestamp: Timestamp,
    ) -> Result<SpatialCache, Error>;
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

    // TODO: handle backing sources
    pub async fn get_spatial_data(
        &self,
        polygon: Vec<GeoPoint>,
        extra_spec: &str,
        timestamp: Timestamp,
    ) -> Result<SpatialCache, Error> {

        let (data_source_id, element) = extra_spec
        .split_once(':')
        .ok_or_else(|| Error::InvalidSeriesId(extra_spec.to_string()))?;

        let data_source = self
            .sources
            .get(data_source_id)
            .ok_or_else(|| Error::InvalidDataSource(data_source_id.to_string()))?;

        data_source.get_spatial_data(polygon, element, timestamp).await
    }
}
