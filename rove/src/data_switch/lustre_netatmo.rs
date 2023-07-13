use crate::util::Timestamp;
use chrono::prelude::*;
use csv;
use std::fs::File;

pub fn read_netatmo(time: Timestamp) {
    let time = Utc.timestamp_opt(time.0, 0).unwrap();

    // TODO: assert minute and second are both 0

    let path = format!("{}", time.format("/lustre/storeB/immutable/archive/projects/metproduction/yr_short/%Y/%m/%d/obs_ta_%Y%m%dT%HZ.txt"));

    let file = File::open(path).unwrap();

    let mut rdr = csv::ReaderBuilder::new().delimiter(b';').from_reader(file);
    for record_res in rdr.records() {
        let record = record_res.unwrap();
        println!("{:?}", record);
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
        ));
    }
}
