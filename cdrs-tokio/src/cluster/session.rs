use async_trait::async_trait;
use std::collections::HashMap;
use std::marker::PhantomData;
#[cfg(feature = "rust-tls")]
use std::net;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::mpsc::channel as std_channel;
use std::sync::Arc;

use crate::authenticators::{NoneAuthenticatorProvider, SaslAuthenticatorProvider};
use tokio::sync::mpsc::channel;

use crate::cluster::connection_manager::{ConnectionConfig, ConnectionManager};
#[cfg(feature = "rust-tls")]
use crate::cluster::rustls_connection_manager::RustlsConnectionManager;
use crate::cluster::session_data::SessionData;
use crate::cluster::session_worker::{RefreshRequest, SessionWorker};
use crate::cluster::tcp_connection_manager::{NodeTcpConfig, TcpConnectionManager};
#[cfg(feature = "rust-tls")]
use crate::cluster::ClusterRustlsConfig;
#[cfg(feature = "rust-tls")]
use crate::cluster::NodeRustlsConfigBuilder;
use crate::cluster::SessionPager;
use crate::cluster::{CdrsSession, GetConnection, GetRetryPolicy, KeyspaceHolder};
use crate::compression::Compression;
use crate::error;
use crate::events::{new_listener, EventStream, EventStreamNonBlocking, Listener};
use crate::frame::events::SimpleServerEvent;
use crate::frame::Frame;
use crate::load_balancing::LoadBalancingStrategy;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::retry::{
    DefaultRetryPolicy, ExponentialReconnectionPolicy, NeverReconnectionPolicy, ReconnectionPolicy,
    RetryPolicy,
};
#[cfg(feature = "rust-tls")]
use crate::transport::TransportRustls;
use crate::transport::{CdrsTransport, TransportTcp};
use arc_swap::ArcSwap;
use futures::future::{FutureExt, RemoteHandle};

static NEVER_RECONNECTION_POLICY: NeverReconnectionPolicy = NeverReconnectionPolicy;

pub const DEFAULT_TRANSPORT_BUFFER_SIZE: usize = 1024;

/// CDRS session that holds a pool of connections to nodes.
pub struct Session<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T>,
    CF: ConnectionConfig<T, CM>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
> {
    load_balancing: LB,
    session_data: Arc<ArcSwap<SessionData<CM>>>,
    node_config: CF,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
    _worker_handle: RemoteHandle<()>,
    _transport: PhantomData<T>,
    _connection_manager: PhantomData<CM>,
}

impl<
        'a,
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > Session<T, CM, CF, LB>
{
    /// Basing on current session returns new `SessionPager` that can be used
    /// for performing paged queries.
    pub fn paged(&'a self, page_size: i32) -> SessionPager<'a, Session<T, CM, CF, LB>, T>
    where
        Session<T, CM, CF, LB>: CdrsSession<T>,
    {
        SessionPager::new(self, page_size)
    }

    fn get_data(&self) -> Arc<SessionData<CM>> {
        self.session_data.load_full()
    }

    fn new(
        load_balancing: LB,
        session_data: Arc<ArcSwap<SessionData<CM>>>,
        node_config: CF,
        worker_handle: RemoteHandle<()>,
        refresh_channel: tokio::sync::mpsc::Sender<RefreshRequest>,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        Session {
            load_balancing,
            session_data,
            node_config,
            retry_policy,
            refresh_channel,
            _worker_handle: worker_handle,
            reconnection_policy,
            _transport: Default::default(),
            _connection_manager: Default::default(),
        }
    }
}

#[async_trait]
impl<
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > GetConnection<T> for Session<T, CM, CF, LB>
{
    async fn load_balanced_connection(&self) -> Option<error::Result<Arc<T>>> {
        // when using a load balancer with > 1 node, don't use reconnection policy for a given node,
        // but jump to the next one

        let data = self.get_data();

        let connection_manager = {
            if self.load_balancing.size(&data) < 2 {
                self.load_balancing.next(&data)
            } else {
                None
            }
        };

        if let Some(connection_manager) = connection_manager {
            let connection = connection_manager
                .connection(self.reconnection_policy.deref())
                .await;

            return match connection {
                Ok(connection) => Some(Ok(connection)),
                Err(error) => Some(Err(error)),
            };
        }

        loop {
            let connection_manager = self.load_balancing.next(&data)?;
            let connection = connection_manager
                .connection(&NEVER_RECONNECTION_POLICY)
                .await;
            if let Ok(connection) = connection {
                return Some(Ok(connection));
            }
        }
    }

    async fn node_connection(&self, _node: &SocketAddr) -> Option<error::Result<Arc<T>>> {
        todo!();
    }
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > QueryExecutor<T> for Session<T, CM, CF, LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > PrepareExecutor<T> for Session<T, CM, CF, LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > ExecExecutor<T> for Session<T, CM, CF, LB>
{
}

#[async_trait]
impl<
        'a,
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > BatchExecutor<T> for Session<T, CM, CF, LB>
{
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T>,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > GetRetryPolicy for Session<T, CM, CF, LB>
{
    fn retry_policy(&self) -> &dyn RetryPolicy {
        self.retry_policy.as_ref()
    }
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        CF: ConnectionConfig<T, CM> + Send + Sync,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > CdrsSession<T> for Session<T, CM, CF, LB>
{
}

/// Workaround for <https://github.com/rust-lang/rust/issues/63033>
#[repr(transparent)]
pub struct RetryPolicyWrapper(pub Box<dyn RetryPolicy + Send + Sync>);

#[repr(transparent)]
pub struct ReconnectionPolicyWrapper(pub Box<dyn ReconnectionPolicy + Send + Sync>);

// /// Creates new session that will perform queries without any compression. `Compression` type
// /// can be changed at any time.
// /// As a parameter it takes:
// /// * cluster config
// /// * load balancing strategy (cannot be changed during `Session` life time).
// #[deprecated(note = "Use SessionBuilder instead.")]
// pub async fn new<LB>(
//     nodes: Vec<SocketAddr>,
//     load_balancing: LB,
//     retry_policy: Box<dyn RetryPolicy + Send + Sync>,
//     reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
// ) -> error::Result<Session<TransportTcp, TcpConnectionManager, NodeTcpConfig, LB>>
// where
//     LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
// {
//     Ok(TcpSessionBuilder::new(load_balancing, nodes)
//         .with_retry_policy(retry_policy)
//         .with_reconnection_policy(reconnection_policy)
//         .build())
// }

// /// Creates new session that will perform queries with Snappy compression. `Compression` type
// /// can be changed at any time.
// /// As a parameter it takes:
// /// * cluster config
// /// * load balancing strategy (cannot be changed during `Session` life time).
// #[deprecated(note = "Use SessionBuilder instead.")]
// pub async fn new_snappy<LB>(
//     nodes: Vec<SocketAddr>,
//     load_balancing: LB,
//     retry_policy: Box<dyn RetryPolicy + Send + Sync>,
//     reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
// ) -> error::Result<Session<TransportTcp, TcpConnectionManager, NodeTcpConfig, LB>>
// where
//     LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
// {
//     Ok(TcpSessionBuilder::new(load_balancing, nodes)
//         .with_compression(Compression::Snappy)
//         .with_retry_policy(retry_policy)
//         .with_reconnection_policy(reconnection_policy)
//         .build())
// }

// /// Creates new session that will perform queries with LZ4 compression. `Compression` type
// /// can be changed at any time.
// /// As a parameter it takes:
// /// * cluster config
// /// * load balancing strategy (cannot be changed during `Session` life time).
// #[deprecated(note = "Use SessionBuilder instead.")]
// pub async fn new_lz4<LB>(
//     nodes: Vec<SocketAddr>,
//     load_balancing: LB,
//     retry_policy: Box<dyn RetryPolicy + Send + Sync>,
//     reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
// ) -> error::Result<Session<TransportTcp, NodeTcpConfig, TcpConnectionManager, LB>>
// where
//     LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync,
// {
//     Ok(TcpSessionBuilder::new(load_balancing, nodes)
//         .with_compression(Compression::Lz4)
//         .with_retry_policy(retry_policy)
//         .with_reconnection_policy(reconnection_policy)
//         .build())
// }

// /// Creates new TLS session that will perform queries without any compression. `Compression` type
// /// can be changed at any time.
// /// As a parameter it takes:
// /// * cluster config
// /// * load balancing strategy (cannot be changed during `Session` life time).
// #[cfg(feature = "rust-tls")]
// #[deprecated(note = "Use SessionBuilder instead.")]
// pub async fn new_tls<LB>(
//     node_configs: &ClusterRustlsConfig,
//     load_balancing: LB,
//     retry_policy: Box<dyn RetryPolicy + Send + Sync>,
//     reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
// ) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
// where
//     LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
// {
//     Ok(
//         RustlsSessionBuilder::new(load_balancing, node_configs.clone())
//             .with_retry_policy(retry_policy)
//             .with_reconnection_policy(reconnection_policy)
//             .build(),
//     )
// }

/// Creates new TLS session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new_snappy_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    Ok(
        RustlsSessionBuilder::new(load_balancing, node_configs.clone())
            .with_compression(Compression::Snappy)
            .with_retry_policy(retry_policy)
            .with_reconnection_policy(reconnection_policy)
            .build(),
    )
}

/// Creates new TLS session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "rust-tls")]
#[deprecated(note = "Use SessionBuilder instead.")]
pub async fn new_lz4_tls<LB>(
    node_configs: &ClusterRustlsConfig,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
) -> error::Result<Session<TransportRustls, RustlsConnectionManager, LB>>
where
    LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync,
{
    Ok(
        RustlsSessionBuilder::new(load_balancing, node_configs.clone())
            .with_compression(Compression::Lz4)
            .with_retry_policy(retry_policy)
            .with_reconnection_policy(reconnection_policy)
            .build(),
    )
}

impl<
        T: CdrsTransport + 'static,
        CM: ConnectionManager<T>,
        CF: ConnectionConfig<T, CM> + Clone + Copy,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > Session<T, CM, CF, LB>
{
    /// Returns new event listener.
    pub async fn listen(
        &self,
        node: SocketAddr,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener, EventStream)> {
        let (event_sender, event_receiver) = channel(256);
        let connection_manager = self.node_config.new_connection_manager_with_auth_and_event(
            node,
            authenticator,
            Some(event_sender),
        );
        let transport = connection_manager
            .connection(&NeverReconnectionPolicy)
            .await?;

        let query_frame = Frame::new_req_register(events);
        transport.write_frame(&query_frame).await?;

        let (sender, receiver) = std_channel();
        Ok((
            new_listener(sender, event_receiver),
            EventStream::new(receiver),
        ))
    }

    // #[cfg(feature = "rust-tls")]
    // pub async fn listen_tls(
    //     &self,
    //     node: net::SocketAddr,
    //     authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    //     events: Vec<SimpleServerEvent>,
    //     dns_name: webpki::DNSName,
    //     config: Arc<rustls::ClientConfig>,
    // ) -> error::Result<(Listener, EventStream)> {
    //     let keyspace_holder = Arc::new(KeyspaceHolder::default());
    //     let config = NodeRustlsConfigBuilder::new(dns_name, config)
    //         .with_node_address(node.into())
    //         .with_authenticator_provider(authenticator)
    //         .build()
    //         .await?;
    //     let (event_sender, event_receiver) = channel(256);
    //     let connection_manager = RustlsConnectionManager::new(
    //         config
    //             .get(0)
    //             .ok_or_else(|| error::Error::General("Empty node list!".into()))?
    //             .clone(),
    //         keyspace_holder,
    //         self.compression,
    //         self.transport_buffer_size,
    //         self.tcp_nodelay,
    //         Some(event_sender),
    //     );
    //     let transport = connection_manager
    //         .connection(&NeverReconnectionPolicy)
    //         .await?;

    //     let query_frame = Frame::new_req_register(events);
    //     transport.write_frame(&query_frame).await?;

    //     let (sender, receiver) = std_channel();
    //     Ok((
    //         new_listener(sender, event_receiver),
    //         EventStream::new(receiver),
    //     ))
    // }

    pub async fn listen_non_blocking(
        &self,
        node: SocketAddr,
        authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener, EventStreamNonBlocking)> {
        self.listen(node, authenticator, events).await.map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }

    // #[cfg(feature = "rust-tls")]
    // pub async fn listen_tls_blocking(
    //     &self,
    //     node: net::SocketAddr,
    //     authenticator: Arc<dyn SaslAuthenticatorProvider + Send + Sync>,
    //     events: Vec<SimpleServerEvent>,
    //     dns_name: webpki::DNSName,
    //     config: Arc<rustls::ClientConfig>,
    // ) -> error::Result<(Listener, EventStreamNonBlocking)> {
    //     self.listen_tls(node, authenticator, events, dns_name, config)
    //         .await
    //         .map(|l| {
    //             let (listener, stream) = l;
    //             (listener, stream.into())
    //         })
    // }
}

struct SessionConfig<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T>,
    CF: ConnectionConfig<T, CM>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
> {
    initial_peers: Vec<SocketAddr>,
    node_config: CF,
    load_balancing: LB,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    _connection_manager: PhantomData<CM>,
    _transport: PhantomData<T>,
}

impl<
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T>,
        CF: ConnectionConfig<T, CM>,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > SessionConfig<T, CM, CF, LB>
{
    fn new(
        initial_peers: Vec<SocketAddr>,
        node_config: CF,
        load_balancing: LB,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        SessionConfig {
            initial_peers,
            node_config,
            load_balancing,
            retry_policy,
            reconnection_policy,
            _connection_manager: Default::default(),
            _transport: Default::default(),
        }
    }
}

/// Builder for easy `Session` creation. Requires static `LoadBalancingStrategy`, but otherwise, other
/// configuration parameters can be dynamically set. Use concrete implementers to create specific
/// sessions.
pub trait SessionBuilder<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T>,
    CF: ConnectionConfig<T, CM>,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
>
{
    /// Sets new compression.
    fn with_compression(self, compression: Compression) -> Self;

    /// Set new retry policy.
    fn with_retry_policy(self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self;

    /// Set new reconnection policy.
    fn with_reconnection_policy(
        self,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self;

    /// Sets new transport buffer size. High values are recommended with large amounts of in flight
    /// queries.
    fn with_transport_buffer_size(self, transport_buffer_size: usize) -> Self;

    /// Sets NODELAY for given session connections.
    fn with_tcp_nodelay(self, tcp_nodelay: bool) -> Self;

    /// Builds the resulting session.
    fn build(self) -> Session<T, CM, CF, LB>;
}

/// Builder for non-TLS sessions.
pub struct TcpSessionBuilder<LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync> {
    config: SessionConfig<TransportTcp, TcpConnectionManager, NodeTcpConfig, LB>,
}

impl<LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync> TcpSessionBuilder<LB> {
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, nodes: Vec<SocketAddr>) -> Self {
        TcpSessionBuilder {
            config: SessionConfig::new(
                nodes,
                NodeTcpConfig {
                    authenticator_provider: Arc::new(NoneAuthenticatorProvider),
                    keyspace_holder: Arc::new(KeyspaceHolder::default()),
                    compression: Compression::None,
                    buffer_size: DEFAULT_TRANSPORT_BUFFER_SIZE,
                    tcp_nodelay: true,
                    event_handler: None,
                },
                load_balancing,
                Box::new(DefaultRetryPolicy::default()),
                Box::new(ExponentialReconnectionPolicy::default()),
            ),
        }
    }
}

impl<LB: LoadBalancingStrategy<TcpConnectionManager> + Send + Sync>
    SessionBuilder<TransportTcp, TcpConnectionManager, NodeTcpConfig, LB>
    for TcpSessionBuilder<LB>
{
    fn with_compression(mut self, compression: Compression) -> Self {
        self.config.node_config.compression = compression;
        self
    }

    fn with_retry_policy(mut self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self {
        self.config.retry_policy = retry_policy;
        self
    }

    fn with_reconnection_policy(
        mut self,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        self.config.reconnection_policy = reconnection_policy;
        self
    }

    fn with_transport_buffer_size(mut self, transport_buffer_size: usize) -> Self {
        self.config.node_config.buffer_size = transport_buffer_size;
        self
    }

    fn with_tcp_nodelay(mut self, tcp_nodelay: bool) -> Self {
        self.config.node_config.tcp_nodelay = tcp_nodelay;
        self
    }

    fn build(self) -> Session<TransportTcp, TcpConnectionManager, NodeTcpConfig, LB> {
        let mut nodes = Vec::with_capacity(self.config.initial_peers.len());
        let mut known_peers = HashMap::new();

        for addr in self.config.initial_peers {
            let connection_manager =
                TcpConnectionManager::new(addr, self.config.node_config.clone());
            let new_node = Arc::new(connection_manager);
            nodes.push(new_node.clone());
        }

        let session_data = Arc::new(ArcSwap::from(Arc::new(
            SessionData::<TcpConnectionManager>::new(nodes, known_peers),
        )));
        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel::<RefreshRequest>(32);
        let worker = SessionWorker::new(session_data.clone(), refresh_receiver);

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        Session::new(
            self.config.load_balancing,
            session_data,
            self.config.node_config,
            worker_handle,
            refresh_sender,
            self.config.retry_policy,
            self.config.reconnection_policy,
        )
    }
}

#[cfg(feature = "rust-tls")]
/// Builder for TLS sessions.
pub struct RustlsSessionBuilder<LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync> {
    config: SessionConfig<RustlsConnectionManager, LB>,
    node_configs: ClusterRustlsConfig,
}

#[cfg(feature = "rust-tls")]
impl<LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync> RustlsSessionBuilder<LB> {
    /// Creates a new builder with default session configuration.
    pub fn new(load_balancing: LB, node_configs: ClusterRustlsConfig) -> Self {
        RustlsSessionBuilder {
            config: SessionConfig::new(
                Compression::None,
                DEFAULT_TRANSPORT_BUFFER_SIZE,
                true,
                load_balancing,
                Box::new(DefaultRetryPolicy::default()),
                Box::new(ExponentialReconnectionPolicy::default()),
            ),
            node_configs,
        }
    }
}

#[cfg(feature = "rust-tls")]
impl<LB: LoadBalancingStrategy<RustlsConnectionManager> + Send + Sync>
    SessionBuilder<TransportRustls, RustlsConnectionManager, LB> for RustlsSessionBuilder<LB>
{
    fn with_compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    fn with_retry_policy(mut self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) -> Self {
        self.config.retry_policy = retry_policy;
        self
    }

    fn with_reconnection_policy(
        mut self,
        reconnection_policy: Box<dyn ReconnectionPolicy + Send + Sync>,
    ) -> Self {
        self.config.reconnection_policy = reconnection_policy;
        self
    }

    fn with_transport_buffer_size(mut self, transport_buffer_size: usize) -> Self {
        self.config.transport_buffer_size = transport_buffer_size;
        self
    }

    fn with_tcp_nodelay(mut self, tcp_nodelay: bool) -> Self {
        self.config.tcp_nodelay = tcp_nodelay;
        self
    }

    fn build(mut self) -> Session<TransportRustls, RustlsConnectionManager, LB> {
        let keyspace_holder = Arc::new(KeyspaceHolder::default());
        let mut nodes = Vec::with_capacity(self.node_configs.0.len());
        let mut known_peers = HashMap::new();

        for node_config in self.node_configs.0 {
            let connection_manager = RustlsConnectionManager::new(
                node_config,
                keyspace_holder.clone(),
                self.config.compression,
                self.config.transport_buffer_size,
                self.config.tcp_nodelay,
                None,
            );
            nodes.push(Arc::new(connection_manager));
        }

        let session_data = Arc::new(ArcSwap::from(Arc::new(SessionData::<
            RustlsConnectionManager,
        >::new(nodes, known_peers))));
        let (refresh_sender, refresh_receiver) = tokio::sync::mpsc::channel::<RefreshRequest>(32);
        let worker = SessionWorker::new(session_data.clone(), refresh_receiver);

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        Session::new(
            self.config.load_balancing,
            session_data,
            self.config.compression,
            self.config.transport_buffer_size,
            self.config.tcp_nodelay,
            worker_handle,
            refresh_sender,
            self.config.retry_policy,
            self.config.reconnection_policy,
        )
    }
}
