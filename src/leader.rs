use etcd_client::Client;
use tokio::sync::watch;
use tokio::task;
use tracing::{info, warn};

use crate::lease;

pub struct LeaderHandle {
    pub watch: watch::Receiver<LeadershipStatus>,
}

pub enum LeadershipStatus {
    Initial,
    Granted { lease_id: i64 },
    Revoked,
}

pub async fn request_leadership<'a>(client: &'a Client, name: &'static str) -> LeaderHandle {
    let mut client = client.clone();
    info!("leadership started");
    let (tx, rx) = watch::channel(LeadershipStatus::Initial);
    let _ = task::spawn(async move {
        loop {
            let lease = lease::acquire_lease(&client, 1).await.unwrap();
            info!(lease = lease.id, "requesting leadership");
            let response = client.campaign(name, "", lease.id).await.unwrap();
            let key = response.leader().unwrap().key_str().unwrap();
            info!(key, "leadership granted");
            if let Err(_) = tx.send(LeadershipStatus::Granted { lease_id: lease.id }) {
                warn!(lease = lease.id, "error notifying leadership granted");
            }
            if let Err(_) = lease.cancel.await {
                warn!(lease = lease.id, "error waiting for lease expiration");
            }
            info!(key, "leadership revoked");
            if let Err(_) = tx.send(LeadershipStatus::Revoked) {
                warn!(lease = lease.id, "error notifying leadership revoked");
            }
        }
    });
    return LeaderHandle { watch: rx };
}
