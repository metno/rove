use csv;
use std::fs::File;

pub fn read_netatmo() {
    let file = File::open("/lustre/storeB/immutable/archive/projects/metproduction/yr_short/2023/07/13/obs_ta_20230713T00Z.txt").unwrap();

    let mut rdr = csv::Reader::from_reader(file);
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
        read_netatmo();
    }
}
