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
    /// Minimum number of leading points required by all the tests in this pipeline
    #[serde(skip)]
    pub num_leading_required: u8,
    /// Minimum number of trailing points required by all the tests in this pipeline
    #[serde(skip)]
    pub num_trailing_required: u8,
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
    pub max: u8,
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

/// Given a pipeline, derive the number of leading and trailing points per timeseries needed in
/// a dataset, for all the intended data to be QCed by the pipeline
pub fn derive_num_leading_trailing(pipeline: &Pipeline) -> (u8, u8) {
    // TODO: would there be somewhere better for this match to live?
    // perhaps harness or even olympian, but annoyingly it depends on things in this module
    pipeline
        .steps
        .iter()
        .map(|step| match &step.check {
            CheckConf::SpecialValueCheck(_) => (0, 0),
            CheckConf::RangeCheck(_) => (0, 0),
            CheckConf::RangeCheckDynamic(_) => (0, 0),
            CheckConf::StepCheck(_) => (1, 0),
            CheckConf::SpikeCheck(_) => (1, 1),
            CheckConf::FlatlineCheck(conf) => (conf.max, 0),
            CheckConf::BuddyCheck(_) => (0, 0),
            CheckConf::Sct(_) => (0, 0),
            CheckConf::ModelConsistencyCheck(_) => (0, 0),
            CheckConf::Dummy => (0, 0),
        })
        .fold((0, 0), |acc, x| (acc.0.max(x.0), acc.1.max(x.1)))
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

            let mut pipeline = toml::from_str(&std::fs::read_to_string(entry.path())?)?;
            (
                pipeline.num_leading_required,
                pipeline.num_trailing_required,
            ) = derive_num_leading_trailing(&pipeline);

            Ok(Some((name, pipeline)))
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
