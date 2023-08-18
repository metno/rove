pub mod coordinator;
pub mod data_switch;
mod runner;

pub mod pb {
    pub mod util {
        tonic::include_proto!("util");

        impl TryFrom<olympian::Flag> for Flag {
            type Error = &'static str;

            fn try_from(item: olympian::Flag) -> Result<Self, Self::Error> {
                match item {
                    olympian::Flag::Pass => Ok(Self::Pass),
                    olympian::Flag::Fail => Ok(Self::Fail),
                    olympian::Flag::Warn => Ok(Self::Warn),
                    olympian::Flag::Inconclusive => Ok(Self::Inconclusive),
                    olympian::Flag::Invalid => Ok(Self::Invalid),
                    olympian::Flag::DataMissing => Ok(Self::DataMissing),
                    olympian::Flag::Isolated => Ok(Self::Isolated),
                    _ => Err("Unrecognised flag from olympian"),
                }
            }
        }
    }

    pub mod coordinator {
        tonic::include_proto!("coordinator");
    }
}
