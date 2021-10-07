use async_trait::async_trait;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::cluster::connection_manager::{
    startup, ConnectionManager, ThreadSafeReconnectionPolicy,
};
use crate::cluster::{KeyspaceHolder, NodeRustlsConfig};
use crate::compression::Compression;
use crate::error::Result;
use crate::frame::Frame;
use crate::transport::{CdrsTransport, TransportRustls};

/// Single node TLS connection config.
#[derive(Clone)]
pub struct NodeRustlsConfig {
    pub dns_name: webpki::DNSName,
    pub authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    pub tls_config: Arc<rustls::ClientConfig>,
    pub keyspace_holder: Arc<KeyspaceHolder>,
    pub compression: Compression,
    pub buffer_size: usize,
    pub tcp_nodelay: bool,
    pub event_handler: Option<Sender<Frame>>,
}

impl ConnectionConfig<TransportRustls, RustlsConnectionManager> for NodeTcpConfig {
    fn new_connection_manager(self, addr: SocketAddr) -> RustlsConnectionManager {
        RustlsConnectionManager::new(addr, self.clone())
    }

    fn new_connection_manager_with_auth_and_event(
        self,
        addr: SocketAddr,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        event_handler: Option<Sender<Frame>>,
    ) -> RustlsConnectionManager {
        RustlsConnectionManager::new(
            addr,
            NodeRustlsConfig {
                authenticator_provider,
                event_handler,
                ..self.clone()
            },
        )
    }
}

pub struct RustlsConnectionManager {
    addr: SocketAddr,
    config: NodeRustlsConfig,
    connection: RwLock<Option<Arc<TransportRustls>>>,
}

#[async_trait]
impl ConnectionManager<TransportRustls> for RustlsConnectionManager {
    async fn connection(
        &self,
        reconnection_policy: &ThreadSafeReconnectionPolicy,
    ) -> Result<Arc<TransportRustls>> {
        {
            let connection = self.connection.read().await;
            if let Some(connection) = connection.deref() {
                if !connection.is_broken() {
                    return Ok(connection.clone());
                }
            }
        }

        let mut connection = self.connection.write().await;
        if let Some(connection) = connection.deref() {
            if !connection.is_broken() {
                // somebody established connection in the meantime
                return Ok(connection.clone());
            }
        }

        let mut schedule = reconnection_policy.new_node_schedule();

        loop {
            let transport = self.establish_connection().await;
            match transport {
                Ok(transport) => {
                    let transport = Arc::new(transport);
                    *connection = Some(transport.clone());

                    return Ok(transport);
                }
                Err(error) => {
                    let delay = schedule.next_delay().ok_or(error)?;
                    sleep(delay).await;
                }
            }
        }
    }

    fn addr(&self) -> SocketAddr {
        self.config.addr
    }
}

impl RustlsConnectionManager {
    pub fn new(addr: SocketAddr, config: NodeRustlsConfig) -> Self {
        RustlsConnectionManager {
            config,
            connection: Default::default(),
        }
    }

    async fn establish_connection(&self) -> Result<TransportRustls> {
        let transport = TransportRustls::new(
            self.addr,
            self.config.dns_name.clone(),
            self.config.config.clone(),
            self.config.keyspace_holder.clone(),
            self.config.event_handler.clone(),
            self.compression,
            self.buffer_size,
            self.tcp_nodelay,
        )
        .await?;

        startup(
            &transport,
            self.config.authenticator_provider.deref(),
            self.keyspace_holder.deref(),
            self.compression,
        )
        .await?;

        Ok(transport)
    }
}
