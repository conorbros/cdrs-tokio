use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct SessionData<N> {
    pub nodes: Vec<Arc<N>>,
    pub known_peers: HashMap<SocketAddr, Arc<N>>,
}

impl<N> SessionData<N> {
    pub fn new(nodes: Vec<Arc<N>>, known_peers: HashMap<SocketAddr, Arc<N>>) -> SessionData<N> {
        Self { nodes, known_peers }
    }
}
