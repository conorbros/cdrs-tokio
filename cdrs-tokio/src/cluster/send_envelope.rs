use crate::cluster::topology::Node;
use crate::cluster::ConnectionManager;
use crate::retry::{QueryInfo, RetryDecision, RetrySession};
use crate::transport::CdrsTransport;
use cassandra_protocol::envelope::Envelope;
use cassandra_protocol::error;
use std::sync::Arc;

/// Mid-level interface for sending envelopes to the cluster. Uses a query plan to route envelope to
/// appropriate node, and retry policy for error handling. Returns `None` if no nodes were present
/// in the query plan.
pub async fn send_envelope<T: CdrsTransport + 'static, CM: ConnectionManager<T> + 'static>(
    query_plan: impl Iterator<Item = Arc<Node<T, CM>>>,
    envelope: &Envelope,
    is_idempotent: bool,
    mut retry_session: Box<dyn RetrySession + Send + Sync>,
) -> Option<error::Result<Envelope>> {
    'next_node: for node in query_plan {
        loop {
            let transport = node.persistent_connection().await;
            match transport {
                Ok(transport) => match transport.write_envelope(envelope, false).await {
                    Ok(envelope) => return Some(Ok(envelope)),
                    Err(error) => {
                        let query_info = QueryInfo {
                            error: &error,
                            is_idempotent,
                        };

                        match retry_session.decide(query_info) {
                            RetryDecision::RetrySameNode => continue,
                            RetryDecision::RetryNextNode => continue 'next_node,
                            RetryDecision::DontRetry => return Some(Err(error)),
                        }
                    }
                },
                Err(error) => return Some(Err(error)),
            }
        }
    }

    None
}
