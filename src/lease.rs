use std::time::Duration;

use anyhow::{anyhow, Result};
use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper};
use tokio::task;
use tokio::time::delay_for;
use tracing::Instrument;
use tracing::{debug, info, span, warn, Level};

pub type LeaseId = i64;

pub async fn acquire_lease(client: &Client, ttl: i64) -> Result<LeaseId> {
    info!(ttl, "acquiring lease");
    let mut client = client.clone();
    let grant_response = client.lease_grant(ttl, None).await?;
    let lease_id = grant_response.id();
    let (keeper, stream) = client.lease_keep_alive(lease_id).await?;
    let _join = task::spawn(async move {
        let span = span!(Level::INFO, "keep alive", lease_id);
        lease_keep_alive(keeper, stream)
            .instrument(span)
            .await
            .expect("error handling lease")
    });
    Ok(lease_id)
}

async fn lease_keep_alive(mut req: LeaseKeeper, mut res: LeaseKeepAliveStream) -> Result<()> {
    info!("begin");
    loop {
        req.keep_alive().await?;
        let message = res
            .message()
            .await?
            .ok_or_else(|| anyhow!("no keepalive response"))?;
        let ttl = message.ttl();
        if ttl == 0 {
            warn!("lease expired");
            break;
        }
        debug!(ttl, "send keep alive");
        delay_for(Duration::from_secs_f64((ttl as f64) / 2.0)).await;
    }
    info!("end");
    Ok(())
}
