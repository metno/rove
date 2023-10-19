//! Utilities for creating and using [`DataConnector`](crate::data_switch::DataConnector)s
//!
//! Implementations of the [`DataConnector`](crate::data_switch::DataConnector)
//! trait are how ROVE accesses to data for QC. For any data source you wish ROVE to be able to pull data from, you must write an implementation of
//! [`DataConnector`](crate::data_switch::DataConnector) for it, and load that
//! connector into a [`DataSwitch`], which you then pass to
//! [`start_server`](crate::start_server) if using ROVE in gRPC
//! mode, or [`Scheduler::new`](crate::Scheduler::new)
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
    /// The series_id was not in a valid format
    #[error("series id `{0}` could not be parsed")]
    InvalidSeriesId(String),
    /// The no connector was found for that data_source_id in the DataSwitch
    #[error("data source `{0}` not registered")]
    InvalidDataSource(String),
    /// The DataConnector or its data source could not parse the data_id
    #[error("data id `{data_id}` could not be parsed by data source {data_source}: {source}")]
    InvalidDataId {
        /// Name of the relevant data source
        data_source: &'static str,
        /// The data_id that could not be parsed
        data_id: String,
        /// The error in the DataConnector
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Generic IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The data source was asked for series data but does not offer it
    #[error("this data source does not offer series data: {0}")]
    UnimplementedSeries(String),
    /// The data source was asked for spatial data but does not offer it
    #[error("this data source does not offer spatial data: {0}")]
    UnimplementedSpatial(String),
    /// Failure to join a tokio task
    #[error("tokio task failure")]
    Join(#[from] tokio::task::JoinError),
    /// Catchall for any other errors that might occur inside a DataConnector object
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Unix timestamp, inner i64 is seconds since unix epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub i64);

/// Inclusive range of time, from a start to end [`Timestamp`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timerange {
    /// Start of the timerange
    pub start: Timestamp,
    /// End of the timerange
    pub end: Timestamp,
}

/// Specifier of geographic position, by latitude and longitude
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    /// latitude, in degrees
    pub lat: f32,
    /// longitude, in degrees
    pub lon: f32,
}

/// A geospatial polygon
///
/// represented by its vertices as a sequence of lat-lon points
pub type Polygon = [GeoPoint];

/// Container of series data
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesCache {
    /// Time of the first observation in data
    pub start_time: Timestamp,
    /// Period of the timeseries, i.e. the time gap between successive elements
    pub period: RelativeDuration,
    /// Data points of the timeseries in chronological order
    ///
    /// `None`s represent gaps in the series
    pub data: Vec<Option<f32>>,
    /// The number of extra points in the series before the data to be QCed
    ///
    /// These points are needed because certain timeseries tests need more
    /// context around points to be able to QC them. The scheduler looks at
    /// the list of requested tests to figure out how many leading points will
    /// be needed, and requests a SeriesCache from the DataSwitch with that
    /// number of leading points
    pub num_leading_points: u8,
}

/// Container of spatial data
///
/// a [`new`](SpatialCache::new) method is provided to
/// avoid the need to construct an R*-tree manually
#[derive(Debug, Clone)]
pub struct SpatialCache {
    /// an [R*-tree](https://en.wikipedia.org/wiki/R*-tree) used to spatially
    /// index the data
    pub rtree: SpatialTree,
    /// Data points in the spatial slice
    pub data: Vec<f32>,
}

impl SpatialCache {
    /// Create a new SpatialCache without manually constructing the R*-tree
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
/// - fetch_series_data: fetch sequential data, i.e. from a time series
/// - fetch_spatial_data: fetch data that is distributed spatially, at a single
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
///     async fn fetch_series_data(
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
///     async fn fetch_spatial_data(
///         &self,
///         _data_id: &str,
///         // This `Vec` of `GeoPoint`s represents a polygon defining the
///         // area in which data should be fetched. It can be left empty,
///         // in which case the whole data set should be fetched
///         _polygon: &Polygon,
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
    /// fetch sequential data, i.e. from a time series
    async fn fetch_series_data(
        &self,
        data_id: &str,
        timespec: Timerange,
        num_leading_points: u8,
    ) -> Result<SeriesCache, Error>;

    /// fetch data that is distributed spatially, at a single timestamp
    async fn fetch_spatial_data(
        &self,
        data_id: &str,
        polygon: &Polygon,
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
#[derive(Debug, Clone)]
pub struct DataSwitch<'ds> {
    sources: HashMap<&'ds str, &'ds dyn DataConnector>,
}

impl<'ds> DataSwitch<'ds> {
    /// Instantiate a new DataSwitch
    ///
    /// See the DataSwitch struct documentation for more info
    pub fn new(sources: HashMap<&'ds str, &'ds dyn DataConnector>) -> Self {
        Self { sources }
    }

    pub(crate) async fn fetch_series_data(
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
            .fetch_series_data(data_id, timerange, num_leading_points)
            .await
    }

    // TODO: handle backing sources
    pub(crate) async fn fetch_spatial_data(
        &self,
        polygon: &Polygon,
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
            .fetch_spatial_data(data_id, polygon, timestamp)
            .await
    }
}
