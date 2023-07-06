use std::net::SocketAddr;
use tokio_stream::wrappers::UnixListenerStream;

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
        }
    }
}

pub enum ListenerType {
    Addr(SocketAddr),
    UnixListener(UnixListenerStream),
}

/// Unix timestamp, inner i64 is seconds since unix epoch
pub struct Timestamp(pub i64);
