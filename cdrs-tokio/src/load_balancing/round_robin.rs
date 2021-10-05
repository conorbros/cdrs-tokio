use crate::cluster::SessionData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::LoadBalancingStrategy;

/// Load balancing strategy based on round-robin.
#[derive(Debug)]
pub struct RoundRobin {
    prev_idx: AtomicUsize,
}

impl RoundRobin {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Default for RoundRobin {
    fn default() -> Self {
        RoundRobin {
            prev_idx: Default::default(),
        }
    }
}

// impl From<Vec<Arc<N>>> for RoundRobin {
//     fn from(cluster: Vec<Arc<N>>) -> RoundRobin {
//         RoundRobin {
//             prev_idx: AtomicUsize::new(0),
//         }
//     }
// }

impl<N> LoadBalancingStrategy<N> for RoundRobin
where
    N: Sync + Send,
{
    /// Returns next node from a cluster
    fn next<'a>(&self, cluster: &'a SessionData<N>) -> Option<Arc<N>> {
        let cur_idx = self.prev_idx.fetch_add(1, Ordering::SeqCst);
        if let Some(n) = cluster.nodes.get(cur_idx % cluster.nodes.len()) {
            Some(n.clone())
        } else {
            None
        }
    }

    fn size<'a>(&self, cluster: &'a SessionData<N>) -> usize {
        cluster.nodes.len()
    }

    // fn find<'a, F>(&self, mut filter: F, cluster: &'a Vec<N>) -> Option<Box<N>>
    // where
    //     F: FnMut(&N) -> bool,
    // {
    //     if let Some(n) = cluster.iter().find(|node| filter(*node)) {
    //         Some(Box::new(*n))
    //     } else {
    //         None
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin() {
        // let nodes = vec!["a", "b", "c"];
        // let nodes_c = nodes.clone();
        // let load_balancer = RoundRobin::new();
        // //     nodes
        // //         .iter()
        // //         .map(|value| Arc::new(*value))
        // //         .collect::<Vec<Arc<&str>>>(),
        // // );
        // for i in 0..10 {
        //     assert_eq!(&nodes_c[i % 3], load_balancer.next(nodes).unwrap().as_ref());
        // }
    }
}
