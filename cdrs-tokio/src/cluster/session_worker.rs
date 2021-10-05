use crate::cluster::connection_manager::ConnectionManager;
use crate::load_balancing::LoadBalancingStrategy;
use crate::transport::{CdrsTransport, TransportTcp};
use anyhow::Result;

pub struct RefreshRequest {
    response_chan: tokio::sync::oneshot::Sender<Result<()>>,
}

pub struct SessionWorker<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T> + Send,
    LB: LoadBalancingStrategy<CM> + Send + Sync,
> {
    load_balancing: std::sync::Arc<LB>,
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,
    _transport: std::marker::PhantomData<T>,
    _connection_manager: std::marker::PhantomData<CM>,
}

impl<
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T> + Send,
        LB: LoadBalancingStrategy<CM> + Send + Sync,
    > SessionWorker<T, CM, LB>
{
    pub fn new(
        load_balancing: std::sync::Arc<LB>,
        refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,
    ) -> Self {
        Self {
            load_balancing,
            refresh_channel,
            _connection_manager: Default::default(),
            _transport: Default::default(),
        }
    }

    pub async fn work(mut self) {
        use tokio::time::{Duration, Instant};

        let refresh_duration = Duration::from_secs(60); // Refresh topology every 60 seconds
        let mut last_refresh_time = Instant::now();

        loop {
            let mut cur_request: Option<RefreshRequest> = None;

            // Wait until it's time for the next refresh
            let sleep_until: Instant = last_refresh_time
                .checked_add(refresh_duration)
                .unwrap_or_else(Instant::now);

            let sleep_future = tokio::time::sleep_until(sleep_until);
            tokio::pin!(sleep_future);

            tokio::select! {
                _ = sleep_future => {},
                recv_res = self.refresh_channel.recv() => {
                    match recv_res {
                        Some(request) => cur_request = Some(request),
                        None => return, // If refresh_channel was closed then cluster was dropped, we can stop working
                    }
                }
            }

            // Perform the refresh
            println!("Requesting topology refresh");
            last_refresh_time = Instant::now();
            let refresh_res = self.perform_refresh().await;

            // Send refresh result if there was a request
            if let Some(request) = cur_request {
                // We can ignore sending error - if no one waits for the response we can drop it
                let _ = request.response_chan.send(refresh_res);
            }
        }
    }

    async fn perform_refresh(&mut self) -> Result<()> {
        // Read latest TopologyInfo
        //let topo_info = self.topology_reader.read_topology_info().await?;

        //println!("{:?}", topo_info);

        Ok(())
    }
}
