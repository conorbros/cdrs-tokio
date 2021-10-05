use crate::cluster::session_data::SessionData;
use std::sync::Arc;

mod random;
mod round_robin;
mod single_node;

//pub use crate::load_balancing::random::Random;
pub use crate::load_balancing::round_robin::RoundRobin;
//pub use crate::load_balancing::single_node::SingleNode;

/// Load balancing strategy, usually used for managing target node connections.
pub trait LoadBalancingStrategy<N> {
    fn next<'a>(&self, session_data: &'a SessionData<N>) -> Option<Arc<N>>;
    fn size<'a>(&self, cluster: &'a SessionData<N>) -> usize;

    // fn find<'a, F>(&self, filter: F, cluster: &'a Vec<N>) -> Option<Arc<N>>
    // where
    //     F: FnMut(&N) -> bool;
}
