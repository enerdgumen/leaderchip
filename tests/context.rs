use etcd_client::Client;
use once_cell::sync::OnceCell;
use testcontainers::images::generic::{GenericImage, Stream, WaitFor};
use testcontainers::{clients, Container, Docker, Image};
use tracing::Level;

pub struct TestContext<'a> {
    pub etcd: Client,
    _container: Container<'a, clients::Cli, GenericImage>,
}

impl<'a> TestContext<'a> {
    pub async fn new() -> TestContext<'a> {
        static TRACING: OnceCell<()> = OnceCell::new();
        static DOCKER: OnceCell<clients::Cli> = OnceCell::new();
        TRACING.get_or_init(setup_tracing);
        let docker = DOCKER.get_or_init(clients::Cli::default);
        let container = docker.run(etcd_image());
        let port = container.get_host_port(2379).unwrap();
        let endpoint = format!("127.0.0.1:{}", port);
        let etcd = Client::connect([endpoint], None).await.unwrap();
        TestContext {
            etcd,
            _container: container,
        }
    }
}

fn setup_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

fn etcd_image() -> GenericImage {
    GenericImage::new("quay.io/coreos/etcd:v3.4.13")
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
        })
}
