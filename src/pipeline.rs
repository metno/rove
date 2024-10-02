use serde::Deserialize;
use std::{collections::HashMap, path::Path};
use thiserror::Error;
use toml;

#[derive(Debug, Deserialize, PartialEq)]
struct Pipeline {
    steps: Vec<PipelineElement>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct PipelineElement {
    name: String,
    test: TestConf,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TestConf {
    SpecialValueCheck(SpecialValueCheckConf),
    RangeCheck(RangeCheckConf),
    RangeCheckDynamic(RangeCheckDynamicConf),
    StepCheck(StepCheckConf),
    SpikeCheck(SpikeCheckConf),
    FlatlineCheck(FlatlineCheckConf),
    BuddyCheck(BuddyCheckConf),
    Sct(SctConf),
    ModelConsistencyCheck(ModelConsistencyCheckConf),
}

#[derive(Debug, Deserialize, PartialEq)]
struct SpecialValueCheckConf {
    special_values: Vec<f32>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct RangeCheckConf {
    max: f32,
    min: f32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct RangeCheckDynamicConf {
    source: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct StepCheckConf {
    max: f32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct SpikeCheckConf {
    max: f32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct FlatlineCheckConf {
    max: i32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct BuddyCheckConf {
    radii: Vec<f32>,
    nums_min: Vec<u32>,
    threshold: f32,
    max_elev_diff: f32,
    elev_gradient: f32,
    min_std: f32,
    num_iterations: u32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct SctConf {
    num_min: usize,
    num_max: usize,
    inner_radius: f32,
    outer_radius: f32,
    num_iterations: u32,
    num_min_prof: usize,
    min_elev_diff: f32,
    min_horizontal_scale: f32,
    vertical_scale: f32,
    pos: Vec<f32>,
    neg: Vec<f32>,
    eps2: Vec<f32>,
    obs_to_check: Option<Vec<bool>>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct ModelConsistencyCheckConf {
    model_source: String,
    model_args: String,
    threshold: f32,
}

#[derive(Error, Debug)]
pub enum Error {
    /// Generic IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// TOML deserialize error
    #[error("failed to deserialize toml: {0}")]
    TomlDeserialize(#[from] toml::de::Error),
    /// The directory contained something that wasn't a file
    #[error("the directory contained something that wasn't a file")]
    DirectoryStructure,
    /// Pipeline filename could not be parsed as a unicode string
    #[error("pipeline filename could not be parsed as a unicode string")]
    InvalidFilename,
}

fn load_pipelines(path: impl AsRef<Path>) -> Result<HashMap<String, Pipeline>, Error> {
    std::fs::read_dir(path)?
        // transform dir entries into (String, Pipeline) pairs
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(Error::DirectoryStructure);
            }

            let name = entry
                .file_name()
                .to_str()
                .ok_or(Error::InvalidFilename)?
                .trim_end_matches(".toml")
                .to_string();
            let elems = toml::from_str(&std::fs::read_to_string(entry.path())?)?;

            Ok(Some((name, elems)))
        })
        // remove `None`s
        .filter_map(Result::transpose)
        // collect to hash map
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize_fresh() {
        load_pipelines("sample_pipelines/fresh")
            .unwrap()
            .get("TA_PT1H")
            .unwrap();
    }
}
