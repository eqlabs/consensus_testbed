mod consensus;
mod message;
mod node;

use crate::consensus::Consensus;
use crate::node::Node;
use pea2pea::{connect_nodes, Topology};
use std::time::Duration;
use tokio::time::sleep;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

// The number of nodes to spawn.
const NUM_NODES: u16 = 4;
// The amount of time in seconds the nodes are to be running.
const RUNTIME_SECS: u64 = 60;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    // Start the logger.
    start_logger(LevelFilter::INFO);

    // Create a number of Aleo nodes.
    let mut nodes = Vec::with_capacity(NUM_NODES as usize);
    for _ in 0..NUM_NODES as usize {
        nodes.push(Node::new("node_name".to_string(), NUM_NODES - 1).await);
    }

    // Connect the nodes in a dense mesh.
    connect_nodes(&nodes, Topology::Mesh).await.unwrap();

    // Start the consensus for all the nodes.
    let mut handles = Vec::new();
    for (i, node) in nodes.into_iter().enumerate() {
        let consensus = Consensus::new(i as u32)?;
        let h = tokio::spawn(async move {
            let (primary, worker) = consensus.start(&node).await.unwrap();
            primary.wait().await;
            worker.wait().await;
        });
        handles.push(h);
    }

    // Wait for a desired amount of time to allow the nodes to do some work.
    for h in handles {
        h.await?;
    }
    sleep(Duration::from_secs(RUNTIME_SECS)).await;
    Ok(())
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
        // .with_file(true)
        // .with_line_number(true)
        // .with_thread_ids(true)
        .with_target(false)
        .init();
}
