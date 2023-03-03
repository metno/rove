use std::net::SocketAddr;
use tokio_stream::wrappers::UnixListenerStream;

pub enum ListenerType {
    Addr(SocketAddr),
    UnixListener(UnixListenerStream),
}
