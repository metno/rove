use std::net::SocketAddr;

pub enum ListenerType {
    Addr(SocketAddr),
    UnixListener,
}
