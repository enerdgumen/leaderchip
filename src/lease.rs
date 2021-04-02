use std::time::Duration;

use anyhow::{anyhow, Result};
use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper};
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::sleep;
use tracing::{debug, info, warn};

pub struct LeaseHandle {
    pub id: i64,
    pub cancel: oneshot::Receiver<()>,
}

struct Lease {
    etcd: Client,
    id: i64,
    ttl: i64,
    keeper: LeaseKeeper,
    keep_alive_stream: LeaseKeepAliveStream,
}

impl Lease {
    async fn new(mut etcd: Client, ttl: i64) -> Result<Lease> {
        let grant_response = etcd.lease_grant(ttl, None).await?;
        let id = grant_response.id();
        let (keeper, stream) = etcd.lease_keep_alive(id).await?;
        Ok(Lease {
            etcd,
            id,
            ttl,
            keeper,
            keep_alive_stream: stream,
        })
    }

    async fn keep_alive(&mut self) -> Result<()> {
        debug!(ttl = self.ttl, "send keep alive");
        self.keeper.keep_alive().await?;
        let message = self.keep_alive_stream.message().await?;
        let response = message.ok_or_else(|| anyhow!("no keepalive response"))?;
        self.ttl = response.ttl();
        Ok(())
    }

    async fn revoke(&mut self) -> Result<()> {
        debug!("revoke");
        self.etcd.lease_revoke(self.id).await?;
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.ttl > 0
    }
}

pub async fn acquire_lease(etcd: &Client, ttl: i64) -> Result<LeaseHandle> {
    info!(ttl, "acquiring lease");
    let lease = Lease::new(etcd.clone(), ttl).await?;
    let lease_id = lease.id;
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let _join = task::spawn(keep_alive_task(lease, cancel_tx));
    Ok(LeaseHandle {
        id: lease_id,
        cancel: cancel_rx,
    })
}

#[tracing::instrument(name="keep-alive", skip(lease, cancel), fields(lease_id=lease.id))]
async fn keep_alive_task(mut lease: Lease, mut cancel: oneshot::Sender<()>) -> Result<()> {
    let proceed = async {
        while lease.is_alive() {
            sleep(Duration::from_secs_f64((lease.ttl as f64) / 2.0)).await;
            lease.keep_alive().await?;
        }
        warn!("lease expired");
        Result::<()>::Ok(())
    };
    tokio::select! {
        _ = proceed => {
            if let Err(_) = cancel.send(()) {
                warn!("failed to notify lease expiration");
            }
        },
        _ = cancel.closed() => {
            info!("cancelled");
            if let Err(e) = lease.revoke().await {
                warn!("failed to revoke lease: {}", e);
            }
        }
    }
    Ok(())
}
