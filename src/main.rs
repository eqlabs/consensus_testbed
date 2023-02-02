mod message;
mod node;

use std::time::Duration;

use pea2pea::{connect_nodes, Topology};
use tokio::time::sleep;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use crate::node::Node;

// The number of nodes to spawn.
const NUM_NODES: u16 = 10;
// The amount of time in seconds the nodes are to be running.
const RUNTIME_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    // Start the logger.
    start_logger(LevelFilter::INFO);

    // Create a number of nodes.
    let mut nodes = Vec::with_capacity(NUM_NODES as usize);
    for _ in 0..NUM_NODES {
        nodes.push(Node::new("node_name".to_string(), NUM_NODES - 1).await);
    }

    // Connect the nodes in a dense mesh.
    connect_nodes(&nodes, Topology::Mesh).await.unwrap();

    // Start the consensus for all the nodes.
    for node in &nodes {
        node.start_consensus();
    }

    // Wait for a desired amount of time to allow the nodes to do some work.
    sleep(Duration::from_secs(RUNTIME_SECS)).await;
}

// Starts logging with the desired log level.
fn start_logger(default_level: LevelFilter) {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter.add_directive("tokio_util=off".parse().unwrap()),
        _ => EnvFilter::default()
            .add_directive(default_level.into())
            .add_directive("tokio_util=off".parse().unwrap()),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .without_time()
        .with_target(false)
        .init();
}
