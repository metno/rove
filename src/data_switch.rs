//! Utilities for creating and using [`DataConnector`](crate::data_switch::DataConnector)s
//!
//! Implementations of the [`DataConnector`](crate::data_switch::DataConnector)
//! trait are how ROVE accesses to data for QC. For any data source you wish ROVE to be able to pull data from, you must write an implementation of
//! [`DataConnector`](crate::data_switch::DataConnector) for it, and load that
//! connector into a [`DataSwitch`], which you then pass to
//! [`server::start_server`](crate::server::start_server) if using ROVE in gRPC
//! mode, or [`scheduler::Scheduler::new`](crate::scheduler::Scheduler::new)
//! otherwise.

use async_trait::async_trait;
use chronoutil::RelativeDuration;
use olympian::SpatialTree;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("series id `{0}` could not be parsed")]
    InvalidSeriesId(String),
    #[error("data source `{0}` not registered")]
    InvalidDataSource(String),
    #[error("data id `{data_id}` could not be parsed by data source {data_source}: {source}")]
    InvalidDataId {
        data_source: &'static str,
        data_id: String,
        source: Box<dyn std::error::Error + Send>,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("this data source does not offer series data: {0}")]
    SeriesUnimplemented(String),
    #[error("this data source does not offer spatial data: {0}")]
    SpatialUnimplemented(String),
    #[error("tokio task failure")]
    JoinError(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send>),
}

/// Unix timestamp, inner i64 is seconds since unix epoch
#[derive(Debug)]
pub struct Timestamp(pub i64);

/// Inclusive range of time, from a start to end [`Timestamp`]
pub struct Timerange {
    pub start: Timestamp,
    pub end: Timestamp,
}

/// Specifier of geographic position, by latitude and longitude
pub use crate::pb::GeoPoint;

/// Container of series data
pub struct SeriesCache {
    pub start_time: Timestamp,
    pub period: RelativeDuration,
    pub data: Vec<Option<f32>>,
    pub num_leading_points: u8,
}

/// Container of spatial data
///
/// a [`new`](SpatialCache::new) method is provided to
/// avoid the need to construct an R*-tree manually
pub struct SpatialCache {
    pub rtree: SpatialTree,
    pub data: Vec<f32>,
}

impl SpatialCache {
    pub fn new(lats: Vec<f32>, lons: Vec<f32>, elevs: Vec<f32>, values: Vec<f32>) -> Self {
        // TODO: ensure vecs have same size
        Self {
            rtree: SpatialTree::from_latlons(lats, lons, elevs),
            data: values,
        }
    }
}

#[async_trait]
pub trait DataConnector: Sync + std::fmt::Debug {
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
        spatial_id: &str,
        timestamp: Timestamp,
    ) -> Result<SpatialCache, Error>;
}

#[derive(Debug)]
pub struct DataSwitch<'ds> {
    sources: HashMap<&'ds str, &'ds dyn DataConnector>,
}

impl<'ds> DataSwitch<'ds> {
    pub fn new(sources: HashMap<&'ds str, &'ds dyn DataConnector>) -> Self {
        Self { sources }
    }

    pub(crate) async fn get_series_data(
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
    pub(crate) async fn get_spatial_data(
        &self,
        polygon: Vec<GeoPoint>,
        spatial_id: &str,
        timestamp: Timestamp,
    ) -> Result<SpatialCache, Error> {
        let (data_source_id, data_id) = spatial_id
            .split_once(':')
            .ok_or_else(|| Error::InvalidSeriesId(spatial_id.to_string()))?;

        let data_source = self
            .sources
            .get(data_source_id)
            .ok_or_else(|| Error::InvalidDataSource(data_source_id.to_string()))?;

        data_source
            .get_spatial_data(polygon, data_id, timestamp)
            .await
    }
}
