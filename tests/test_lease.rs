use std::time::Duration;

use etcd_client::Client as EtcdClient;
use testcontainers::images::generic::{GenericImage, Stream, WaitFor};
use testcontainers::{clients, Container, Docker, Image};
use tokio::time::delay_for;
use tracing::Level;

use leaderchip_etcd::lease;

#[tokio::test]
async fn should_keep_alive_leases() {
    setup_tracing();

    let docker = clients::Cli::default();
    let (mut etcd, _container) = new_etcd_client(&docker).await;
    let lease_id = lease::acquire_lease(&etcd, 1).await.unwrap();
    delay_for(Duration::from_secs(3)).await;
    let leases = etcd.leases().await.unwrap();
    assert_eq!(1, leases.leases().len());
    assert_eq!(lease_id, leases.leases().get(0).unwrap().id());
}

fn setup_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

async fn new_etcd_client(
    docker: &clients::Cli,
) -> (EtcdClient, Container<'_, clients::Cli, GenericImage>) {
    let image = GenericImage::new("quay.io/coreos/etcd:v3.4.13")
        .with_entrypoint("/usr/local/bin/etcd")
        .with_args(
            vec![
                "--name=node1",
                "--initial-advertise-peer-urls=http://127.0.0.1:2380",
                "--listen-peer-urls=http://0.0.0.0:2380",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379",
                "--initial-cluster=node1=http://127.0.0.1:2380",
                "--initial-cluster-state=new",
                "--heartbeat-interval=250",
                "--election-timeout=1250",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
        )
        .with_wait_for(WaitFor::LogMessage {
            message: "serving insecure client requests".into(),
            stream: Stream::StdErr,
        });
    let container = docker.run(image);
    let port = container.get_host_port(2379).unwrap();
    let etcd = EtcdClient::connect([format!("127.0.0.1:{}", port)], None)
        .await
        .unwrap();
    (etcd, container)
}
