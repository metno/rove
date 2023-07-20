use async_trait::async_trait;
use chrono::prelude::*;
use rove::{
    data_switch,
    data_switch::{DataSource, SeriesCache, SpatialCache, Timerange, Timestamp},
};
use std::fs::File;

#[derive(Debug)]
pub struct LustreNetatmo;

fn read_netatmo(time: Timestamp) -> Result<SpatialCache, data_switch::Error> {
    let time = Utc.timestamp_opt(time.0, 0).unwrap();

    // TODO: assert minute and second are both 0

    let path = format!("{}", time.format("/lustre/storeB/immutable/archive/projects/metproduction/yr_short/%Y/%m/%d/obs_ta_%Y%m%dT%HZ.txt"));

    let file = File::open(path).unwrap();

    let mut rdr = csv::ReaderBuilder::new().delimiter(b';').from_reader(file);
    for record_res in rdr.records() {
        let record = record_res.unwrap();
        println!("{:?}", record);
    }

    // TODO: actually populate these vecs
    Ok(SpatialCache::new(vec![], vec![], vec![], vec![]))
}

#[async_trait]
impl DataSource for LustreNetatmo {
    async fn get_series_data(
        &self,
        _data_id: &str,
        _timespec: Timerange,
        _num_leading_points: u8,
    ) -> Result<SeriesCache, data_switch::Error> {
        // TODO: return a nice error instead of panicking
        unimplemented!()
    }

    async fn get_spatial_data(&self, time: Timestamp) -> Result<SpatialCache, data_switch::Error> {
        // TODO: spawn this in a blocking tokio thread
        read_netatmo(time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_netatmo() {
        read_netatmo(Timestamp(
            Utc.with_ymd_and_hms(2023, 07, 13, 0, 0, 0)
                .unwrap()
                .timestamp(),
        ))
        .unwrap();
    }
}
