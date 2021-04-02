use std::time::Duration;

use tokio::time::sleep;

use context::TestContext;
use leaderchip_etcd::leader::{request_leadership, LeadershipStatus};

mod context;

#[tokio::test]
async fn can_become_leader() {
    let mut context = TestContext::new().await;

    let instance1 = request_leadership(&context.etcd, "srv");
    sleep(Duration::from_secs(1)).await;
    let lease1 = if let LeadershipStatus::Leader { lease_id } = *instance1.watch.borrow() {
        lease_id
    } else {
        panic!("instance1 should be the leader")
    };

    let instance2 = request_leadership(&context.etcd, "srv");
    sleep(Duration::from_secs(1)).await;
    assert!(matches!(
        *instance2.watch.borrow(),
        LeadershipStatus::Requested
    ));

    context.etcd.lease_revoke(lease1).await.unwrap();
    sleep(Duration::from_secs(1)).await;
    assert!(matches!(
        *instance1.watch.borrow(),
        LeadershipStatus::Follower
    ));
    assert!(matches!(
        *instance2.watch.borrow(),
        LeadershipStatus::Leader { .. }
    ));
}

#[tokio::test]
async fn drop_handle_give_up_leadership() {
    let context = TestContext::new().await;

    // given a leader instance
    let instance1 = request_leadership(&context.etcd, "srv");
    sleep(Duration::from_secs(1)).await;

    // ...and a follower
    let instance2 = request_leadership(&context.etcd, "srv");
    sleep(Duration::from_secs(1)).await;

    // when the leader gives up its status
    drop(instance1);
    sleep(Duration::from_secs(3)).await;

    // then the follower is promoted to leader
    assert!(matches!(
        *instance2.watch.borrow(),
        LeadershipStatus::Leader { .. }
    ));
}
