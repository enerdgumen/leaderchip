use std::time::Duration;

use tokio::time::sleep;

use context::TestContext;
use leaderchip_etcd::leader::{request_leadership, LeadershipStatus};

mod context;

#[tokio::test]
async fn can_become_leader() {
    let mut context = TestContext::new().await;

    let instance1 = request_leadership(&context.etcd, "srv").await;
    sleep(Duration::from_secs(1)).await;
    let lease1 = if let LeadershipStatus::Granted { lease_id } = *instance1.watch.borrow() {
        lease_id
    } else {
        panic!("instance1 should be the leader")
    };

    let instance2 = request_leadership(&context.etcd, "srv").await;
    sleep(Duration::from_secs(1)).await;
    assert!(matches!(
        *instance2.watch.borrow(),
        LeadershipStatus::Initial
    ));

    context.etcd.lease_revoke(lease1).await.unwrap();
    sleep(Duration::from_secs(1)).await;
    assert!(matches!(
        *instance1.watch.borrow(),
        LeadershipStatus::Revoked
    ));
    assert!(matches!(
        *instance2.watch.borrow(),
        LeadershipStatus::Granted { .. }
    ));
}
