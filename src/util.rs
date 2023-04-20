use std::net::SocketAddr;
use tokio_stream::wrappers::UnixListenerStream;

pub enum ListenerType {
    Addr(SocketAddr),
    UnixListener(UnixListenerStream),
}

pub fn flag_int_to_string(int: i32) -> String {
    match int {
        3 => "Inconclusive".to_string(),
        _ => unimplemented!(),
    }
}
