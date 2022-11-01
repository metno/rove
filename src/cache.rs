use titanlib_rs::points::Points;

pub fn get_timeseries_data(_station_id: u32, _unix_timestamp: i64) -> [f32; 3] {
    return [1., 1., 1.]; // TODO get actual data
}

pub fn get_spatial_data(_station_id: u32, _unix_timestamp: i64) -> Points {
    todo!()
}
