use etcd_client::Client;
use tokio::sync::{oneshot, watch};
use tokio::{select, task};
use tracing::{info, warn};

use crate::lease;

pub struct LeaderHandle {
    pub watch: watch::Receiver<LeadershipStatus>,
    _cancel: oneshot::Receiver<()>,
}

pub enum LeadershipStatus {
    Initial,
    Granted { lease_id: i64 },
    Revoked,
}

pub fn request_leadership(etcd: &Client, name: &'static str) -> LeaderHandle {
    let (watch_tx, watch_rx) = watch::channel(LeadershipStatus::Initial);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let _ = task::spawn(handle_leadership(etcd.clone(), name, watch_tx, cancel_tx));
    return LeaderHandle {
        watch: watch_rx,
        _cancel: cancel_rx,
    };
}

#[tracing::instrument(name = "leadership", skip(etcd, watch, cancel))]
async fn handle_leadership(
    mut etcd: Client,
    name: &str,
    watch: watch::Sender<LeadershipStatus>,
    mut cancel: oneshot::Sender<()>,
) {
    let proceed = async {
        loop {
            let lease = lease::acquire_lease(&etcd, 1).await.unwrap();
            info!(lease = lease.id, "requesting");
            let response = etcd.campaign(name, "", lease.id).await.unwrap();
            let key = response.leader().unwrap().key_str().unwrap();
            info!(key, "granted");
            if let Err(_) = watch.send(LeadershipStatus::Granted { lease_id: lease.id }) {
                warn!(lease = lease.id, "error notifying leadership granted");
            }
            if let Err(_) = lease.cancel.await {
                warn!(lease = lease.id, "error waiting for lease expiration");
            }
            info!(key, "revoked");
            if let Err(_) = watch.send(LeadershipStatus::Revoked) {
                warn!(lease = lease.id, "error notifying leadership revoked");
            }
        }
    };
    select! {
        _ = proceed => {},
        _ = cancel.closed() => {
            info!("give up leadership");
        }
    }
}
