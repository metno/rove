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

/// Error type for DataSwitch
///
/// When implementing DataConnector, it may be helpful to implement your own
/// internal Error type, but it must ultimately be mapped to this type before
/// returning
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

/// Trait for pulling data from data sources
///
/// Uses [mod@async_trait]. It is recommended to tag your implementation with
/// the [`macro@async_trait`] macro to avoid having to deal with pinning,
/// futures, and lifetimes manually. This trait has two required methods:
///
/// - get_series_data: fetch sequential data, i.e. from a time series
/// - get_spatial_data: fetch data that is distributed spatially, at a single
/// timestamp
///
/// Here is an example implementation that just returns dummy data:
///
/// ```
/// use async_trait::async_trait;
/// use chronoutil::RelativeDuration;
/// use rove::data_switch::{self, *};
///
/// // You can use the receiver type to store anything that should persist
/// // between requests, i.e a connection pool
/// #[derive(Debug)]
/// struct TestDataSource;
///
/// #[async_trait]
/// impl DataConnector for TestDataSource {
///     async fn get_series_data(
///         &self,
///         // This is the part of spatial_id after the colon. You can
///         // define its format any way you like, and use it to
///         // determine what to fetch from the data source.
///         _data_id: &str,
///         // The timerange in the series that data is needed from.
///         _timespec: Timerange,
///         // Some timeseries QC tests require extra data from before
///         // the start of the timerange to function. ROVE determines
///         // how many extra data points are needed, and passes that in
///         // here.
///         num_leading_points: u8,
///     ) -> Result<SeriesCache, data_switch::Error> {
///         // Here you can do whatever is need to fetch real data, whether
///         // that's a REST request, SQL call, NFS read etc.
///
///         Ok(SeriesCache {
///             start_time: Timestamp(0),
///             period: RelativeDuration::minutes(5),
///             data: vec![Some(1.); 1],
///             num_leading_points,
///         })
///     }
///
///     async fn get_spatial_data(
///         &self,
///         _data_id: &str,
///         // This `Vec` of `GeoPoint`s represents a polygon defining the
///         // area in which data should be fetched. It can be left empty,
///         // in which case the whole data set should be fetched
///         _polygon: Vec<GeoPoint>,
///         // Unix timestamp representing the time of the data to be fetched
///         _timestamp: Timestamp,
///     ) -> Result<SpatialCache, data_switch::Error> {
///         // As above, calls to the data source to get real data go here
///
///         Ok(SpatialCache::new(
///             vec![1.; 1],
///             vec![1.; 1],
///             vec![1.; 1],
///             vec![1.; 1],
///         ))
///     }
/// }
/// ```
///
/// Some real implementations can be found in [rove/met_connectors](https://github.com/metno/rove/tree/trunk/met_connectors)
#[async_trait]
pub trait DataConnector: Sync + std::fmt::Debug {
    async fn get_series_data(
        &self,
        data_id: &str,
        timespec: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, Error>;

    async fn get_spatial_data(
        &self,
        data_id: &str,
        polygon: Vec<GeoPoint>,
        timestamp: Timestamp,
    ) -> Result<SpatialCache, Error>;
}

/// Data routing utility for ROVE
///
/// This contains a map of &str to [`DataConnector`]s and is used by ROVE to
/// pull data for QC tests the &str keys in the hashmap correspond to the data
/// source name you would use in the call to validate_series or validate_spatial.
/// So in the example below, you would contruct a spatial_id like "test:single"
/// to direct ROVE to fetch data from `TestDataSource` and pass it the data_id
/// "single"
///
/// ```
/// use rove::{
///     data_switch::{DataConnector, DataSwitch},
///     dev_utils::TestDataSource,
/// };
/// use std::collections::HashMap;
///
/// let data_switch = DataSwitch::new(HashMap::from([
///     ("test", &TestDataSource{
///         data_len_single: 3,
///         data_len_series: 1000,
///         data_len_spatial: 1000,
///     } as &dyn DataConnector),
/// ]));
/// ```
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
            .get_spatial_data(data_id, polygon, timestamp)
            .await
    }
}
