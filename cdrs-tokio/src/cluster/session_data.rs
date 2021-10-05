use std::sync::Arc;

pub struct SessionData<N> {
    pub nodes: Vec<Arc<N>>,
}

impl<N> SessionData<N> {
    pub fn new(nodes: Vec<Arc<N>>) -> SessionData<N> {
        Self { nodes }
    }
}
