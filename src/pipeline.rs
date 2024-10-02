use serde::Deserialize;
use std::{collections::HashMap, path::Path};
use thiserror::Error;

/// Data structure defining a pipeline of checks, with parameters built in
///
/// Rather than constructing these manually, a convenience function `load_pipelines` is provided
/// to deserialize a set of pipelines from a directory containing TOML files defining them.
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct Pipeline {
    /// Sequence of steps in the pipeline
    pub steps: Vec<PipelineStep>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct PipelineStep {
    pub name: String,
    pub check: CheckConf,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum CheckConf {
    SpecialValueCheck(SpecialValueCheckConf),
    RangeCheck(RangeCheckConf),
    RangeCheckDynamic(RangeCheckDynamicConf),
    StepCheck(StepCheckConf),
    SpikeCheck(SpikeCheckConf),
    FlatlineCheck(FlatlineCheckConf),
    BuddyCheck(BuddyCheckConf),
    Sct(SctConf),
    ModelConsistencyCheck(ModelConsistencyCheckConf),
    #[serde(skip)]
    Dummy,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct SpecialValueCheckConf {
    pub special_values: Vec<f32>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct RangeCheckConf {
    pub max: f32,
    pub min: f32,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct RangeCheckDynamicConf {
    pub source: String,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct StepCheckConf {
    pub max: f32,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct SpikeCheckConf {
    pub max: f32,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct FlatlineCheckConf {
    pub max: i32,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct BuddyCheckConf {
    pub radii: Vec<f32>,
    pub nums_min: Vec<u32>,
    pub threshold: f32,
    pub max_elev_diff: f32,
    pub elev_gradient: f32,
    pub min_std: f32,
    pub num_iterations: u32,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct SctConf {
    pub num_min: usize,
    pub num_max: usize,
    pub inner_radius: f32,
    pub outer_radius: f32,
    pub num_iterations: u32,
    pub num_min_prof: usize,
    pub min_elev_diff: f32,
    pub min_horizontal_scale: f32,
    pub vertical_scale: f32,
    pub pos: Vec<f32>,
    pub neg: Vec<f32>,
    pub eps2: Vec<f32>,
    pub obs_to_check: Option<Vec<bool>>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct ModelConsistencyCheckConf {
    pub model_source: String,
    pub model_args: String,
    pub threshold: f32,
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

/// Given a directory containing toml files that each define a check pipeline, construct a hashmap
/// of pipelines, where the keys are the pipelines' names (filename of the toml file that defines
/// them, without the file extension)
pub fn load_pipelines(path: impl AsRef<Path>) -> Result<HashMap<String, Pipeline>, Error> {
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
