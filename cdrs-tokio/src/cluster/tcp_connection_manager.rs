use crate::authenticators::SaslAuthenticatorProvider;
use crate::cluster::connection_manager::{
    startup, ConnectionConfig, ConnectionManager, ThreadSafeReconnectionPolicy,
};
use crate::cluster::KeyspaceHolder;
use crate::compression::Compression;
use crate::error::Result;
use crate::frame::Frame;
use crate::transport::{CdrsTransport, TransportTcp};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;

/// Single node TCP connection config.
#[derive(Clone)]
pub struct NodeTcpConfig {
    pub authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    pub keyspace_holder: Arc<KeyspaceHolder>,
    pub compression: Compression,
    pub buffer_size: usize,
    pub tcp_nodelay: bool,
    pub event_handler: Option<Sender<Frame>>,
}

#[async_trait]
impl ConnectionConfig<TransportTcp, TcpConnectionManager> for NodeTcpConfig {
    fn new_connection_manager(self, addr: SocketAddr) -> TcpConnectionManager {
        TcpConnectionManager::new(addr, self.clone())
    }

    fn new_connection_manager_with_auth_and_event(
        self,
        addr: SocketAddr,
        authenticator_provider: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        event_handler: Option<Sender<Frame>>,
    ) -> TcpConnectionManager {
        TcpConnectionManager::new(
            addr,
            NodeTcpConfig {
                authenticator_provider,
                event_handler,
                ..self.clone()
            },
        )
    }
}

pub struct TcpConnectionManager {
    addr: SocketAddr,
    config: NodeTcpConfig,
    connection: RwLock<Option<Arc<TransportTcp>>>,
}

#[async_trait]
impl ConnectionManager<TransportTcp> for TcpConnectionManager {
    async fn connection(
        &self,
        reconnection_policy: &ThreadSafeReconnectionPolicy,
    ) -> Result<Arc<TransportTcp>> {
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
        self.addr
    }
}

impl TcpConnectionManager {
    pub fn new(addr: SocketAddr, config: NodeTcpConfig) -> Self {
        TcpConnectionManager {
            addr,
            config,
            connection: Default::default(),
        }
    }

    async fn establish_connection(&self) -> Result<TransportTcp> {
        let transport = TransportTcp::new(
            self.addr,
            self.config.keyspace_holder.clone(),
            self.config.event_handler.clone(),
            self.config.compression,
            self.config.buffer_size,
            self.config.tcp_nodelay,
        )
        .await?;

        startup(
            &transport,
            self.config.authenticator_provider.deref(),
            self.config.keyspace_holder.deref(),
            self.config.compression,
        )
        .await?;

        Ok(transport)
    }
}
