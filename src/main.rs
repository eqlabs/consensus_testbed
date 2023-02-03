mod message;
mod node;

use std::path::Path;
use std::{time::Duration, sync::Arc};

use crypto::PublicKey;
use pea2pea::{connect_nodes, Topology};
use storage::CertificateStore;
use store::rocks::ReadWriteOptions;
use store::{
    reopen,
    rocks::{self, DBMap, MetricConf},
};
use tokio::time::sleep;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use types::{ConsensusStore, Round, CommittedSubDagShell, SequenceNumber, CertificateDigest, Certificate};

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

    let store = make_consensus_store(&Path::new("store"));
    let cert_store = make_certificate_store(&Path::new("cert_store"));
// Start the consensus for all the nodes.
    for node in &nodes {
        node.start_consensus(store.clone(), cert_store.clone());
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

pub fn make_consensus_store(store_path: &std::path::Path) -> Arc<ConsensusStore> {
    const LAST_COMMITTED_CF: &str = "last_committed";
    const SEQUENCE_CF: &str = "sequence";

    let rocksdb = rocks::open_cf(
        store_path,
        None,
        MetricConf::default(),
        &[LAST_COMMITTED_CF, SEQUENCE_CF],
    )
    .expect("Failed to create database");

    let (last_committed_map, sequence_map) = reopen!(&rocksdb,
        LAST_COMMITTED_CF;<PublicKey, Round>,
        SEQUENCE_CF;<SequenceNumber, CommittedSubDagShell>
    );

    Arc::new(ConsensusStore::new(last_committed_map, sequence_map))
}

pub fn make_certificate_store(store_path: &std::path::Path) -> CertificateStore {
    const CERTIFICATES_CF: &str = "certificates";
    const CERTIFICATE_DIGEST_BY_ROUND_CF: &str = "certificate_digest_by_round";
    const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &str = "certificate_digest_by_origin";

    let rocksdb = rocks::open_cf(
        store_path,
        None,
        MetricConf::default(),
        &[
            CERTIFICATES_CF,
            CERTIFICATE_DIGEST_BY_ROUND_CF,
            CERTIFICATE_DIGEST_BY_ORIGIN_CF,
        ],
    )
    .expect("Failed creating database");

    let (certificate_map, certificate_digest_by_round_map, certificate_digest_by_origin_map) = reopen!(&rocksdb,
        CERTIFICATES_CF;<CertificateDigest, Certificate>,
        CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, PublicKey), CertificateDigest>,
        CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(PublicKey, Round), CertificateDigest>);

    CertificateStore::new(
        certificate_map,
        certificate_digest_by_round_map,
        certificate_digest_by_origin_map,
    )
}
