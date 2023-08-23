use async_trait::async_trait;
use chrono::prelude::*;
use rove::pb::util::GeoPoint;
use rove::{
    data_switch,
    data_switch::{DataSource, SeriesCache, SpatialCache, Timerange, Timestamp},
};
use serde::Deserialize;
use std::{fs::File, io};

#[derive(Debug)]
pub struct LustreNetatmo;

#[derive(Debug, Deserialize)]
struct Record {
    lat: f32,
    lon: f32,
    elev: f32,
    value: f32,
    // Provider ID
    // 1=WMO stations, 2=MET Non-WMO stations, 3=Netatmo, 4=Foreign WMO, 5=SVV, 6=Bergensværet, 7=FMI, 8=Luftambulansen, 9=Holfuy, 100=Radar precipitation
    prid: u32,
    // QC flag
    // 0 = OK, >=l = fail
    dqc: u32,
}

fn read_netatmo(time: Timestamp) -> Result<SpatialCache, data_switch::Error> {
    // timestamp should be validated before it gets here, so it should be safe to unwrap
    let time = Utc.timestamp_opt(time.0, 0).unwrap();

    if time.minute() != 0 || time.second() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "timestamps for fetching netatmo data must be on the hour",
        )
        .into());
    }

    let path = format!("{}", time.format("/lustre/storeB/immutable/archive/projects/metproduction/yr_short/%Y/%m/%d/obs_ta_%Y%m%dT%HZ.txt"));

    let file = File::open(path)?;

    // TODO: probably some optimisation potential here?
    let mut lats = Vec::new();
    let mut lons = Vec::new();
    let mut elevs = Vec::new();
    let mut values = Vec::new();

    let mut rdr = csv::ReaderBuilder::new().delimiter(b';').from_reader(file);
    for result in rdr.deserialize() {
        let record: Record = result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // TODO: should we allow more prids?
        // prid 3 represents netatmo data, but if we use this as a backing set
        // I wonder if there's any harm in adding others
        if record.prid == 3 && record.dqc == 0 {
            lats.push(record.lat);
            lons.push(record.lon);
            elevs.push(record.elev);
            values.push(record.value);
        }
    }

    Ok(SpatialCache::new(lats, lons, elevs, values))
}

#[async_trait]
impl DataSource for LustreNetatmo {
    async fn get_series_data(
        &self,
        _data_id: &str,
        _timespec: Timerange,
        _num_leading_points: u8,
    ) -> Result<SeriesCache, data_switch::Error> {
        Err(data_switch::Error::SeriesUnimplemented(
            "netatmo files are only in timeslice format".to_string(),
        ))
    }

    async fn get_spatial_data(
        &self,
        _polygon: Vec<GeoPoint>,
        _extra_spec: &str,
        time: Timestamp,
    ) -> Result<SpatialCache, data_switch::Error> {
        tokio::task::spawn_blocking(move || read_netatmo(time)).await?
    }
}

// TODO: add unit test?
