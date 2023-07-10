use std::net::SocketAddr;
use tokio_stream::wrappers::UnixListenerStream;

// TODO: reconsider this proto mess?
pub mod pb {
    pub mod util {
        tonic::include_proto!("util");

        impl From<olympian::qc_tests::Flag> for Flag {
            fn from(item: olympian::qc_tests::Flag) -> Self {
                match item {
                    olympian::qc_tests::Flag::Pass => Self::Pass,
                    olympian::qc_tests::Flag::Fail => Self::Fail,
                    olympian::qc_tests::Flag::Warn => Self::Warn,
                    olympian::qc_tests::Flag::Inconclusive => Self::Inconclusive,
                    olympian::qc_tests::Flag::Invalid => Self::Invalid,
                    olympian::qc_tests::Flag::DataMissing => Self::DataMissing,
                    olympian::qc_tests::Flag::Isolated => Self::Isolated,
                }
            }
        }
    }

    pub mod coordinator {
        tonic::include_proto!("coordinator");
    }
}

pub enum ListenerType {
    Addr(SocketAddr),
    UnixListener(UnixListenerStream),
}

/// Unix timestamp, inner i64 is seconds since unix epoch
#[derive(Debug)]
pub struct Timestamp(pub i64);
