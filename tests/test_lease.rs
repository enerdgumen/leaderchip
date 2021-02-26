#![feature(once_cell)]

use std::time::Duration;

use tokio::time::sleep;

use context::TestContext;
use leaderchip_etcd::lease;

mod context;

#[tokio::test]
async fn should_keep_alive_leases() {
    let mut ctx = TestContext::new().await;

    // when a lease is acquired
    let lease = lease::acquire_lease(&ctx.etcd, 1).await.unwrap();
    sleep(Duration::from_secs(3)).await;

    // then the lease is kept alive
    let leases = ctx.etcd.leases().await.unwrap();
    assert_eq!(1, leases.leases().len());
    assert_eq!(lease.id, leases.leases().get(0).unwrap().id());
}

#[tokio::test]
async fn dropped_lease_is_not_kept_alive() {
    let mut ctx = TestContext::new().await;

    // given a lease
    let lease = lease::acquire_lease(&ctx.etcd, 1).await.unwrap();
    sleep(Duration::from_secs(2)).await;

    // when the lease is dropped
    drop(lease);

    // then the lease is not alive anymore
    sleep(Duration::from_secs(1)).await;
    let leases = ctx.etcd.leases().await.unwrap();
    assert_eq!(0, leases.leases().len());
}

#[tokio::test]
async fn can_listen_lease_expiration() {
    let mut ctx = TestContext::new().await;

    // given a lease
    let lease = lease::acquire_lease(&ctx.etcd, 1).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    // when the lease expires
    ctx.etcd.lease_revoke(lease.id).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    // the channel is notified
    let result = lease.cancel.await;
    assert_eq!(Ok(()), result);
}
