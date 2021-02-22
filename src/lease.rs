use std::time::Duration;

use anyhow::{anyhow, Result};
use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper};
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::sleep;
use tracing::Instrument;
use tracing::{debug, info, span, warn, Level};

pub struct Lease {
    pub id: i64,
    pub cancel: oneshot::Receiver<()>,
}

pub async fn acquire_lease(client: &Client, ttl: i64) -> Result<Lease> {
    info!(ttl, "acquiring lease");
    let mut client = client.clone();
    let grant_response = client.lease_grant(ttl, None).await?;
    let lease_id = grant_response.id();
    let (keeper, stream) = client.lease_keep_alive(lease_id).await?;
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let _join = task::spawn(async move {
        let span = span!(Level::INFO, "keep-alive", lease_id);
        lease_keep_alive(ttl, keeper, stream, cancel_tx)
            .instrument(span)
            .await
            .expect("error handling lease")
    });
    Ok(Lease {
        id: lease_id,
        cancel: cancel_rx,
    })
}

async fn lease_keep_alive(
    mut ttl: i64,
    mut req: LeaseKeeper,
    mut res: LeaseKeepAliveStream,
    mut cancel: oneshot::Sender<()>,
) -> Result<()> {
    info!("begin");
    let proceed = async {
        while ttl > 0 {
            debug!(ttl, "send keep alive");
            sleep(Duration::from_secs_f64((ttl as f64) / 2.0)).await;
            req.keep_alive().await?;
            let message = res.message().await?;
            let response = message.ok_or_else(|| anyhow!("no keepalive response"))?;
            ttl = response.ttl();
        }
        Result::<()>::Ok(())
    };
    tokio::select! {
        _ = proceed => {
            warn!("lease expired");
            if let Err(_) = cancel.send(()) {
                warn!("failed to notify lease expiration")
            }
        },
        _ = cancel.closed() => {
            info!("cancelled");
        }
    }
    info!("end");
    Ok(())
}
