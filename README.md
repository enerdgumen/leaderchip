# Leaderchip

An early stage library useful for handling leader election in a microservice environment.

It is based on the Tokio runtime and currently only supports Etcd v3.

## Usage

```rust
use etcd_client::Client;

use leaderchip_etcd::leader::{request_leadership, LeadershipStatus};

#[tokio::main]
async fn main() {
    let etcd = Client::connect(["127.0.0.1:2379"], None).await.unwrap();
    let mut handle = request_leadership(&etcd, "srv");
    while handle.watch.changed().await.is_ok() {
        match *handle.watch.borrow() {
            LeadershipStatus::Leader { .. } => {
                println!("I'm the leader");
            }
            LeadershipStatus::Follower => {
                println!("I'm a follower");
            }
        }
    }
}
```

See the integration tests for more examples.

## Licence

MIT
