mod message;
mod node;

use std::{
    borrow::Borrow, collections::{BTreeMap, BTreeSet}, path::Path, str::FromStr, sync::Arc, time::Duration,
};

use config::{Authority, Committee};
use crypto::{NetworkKeyPair, PublicKey};
use fastcrypto::{bls12381::min_sig::BLS12381KeyPair, traits::KeyPair, hash::Hash};
use multiaddr::Multiaddr;
use pea2pea::{connect_nodes, Topology};
use storage::CertificateStore;
use store::{
    reopen,
    rocks::{self, DBMap, MetricConf, ReadWriteOptions},
};
use tokio::time::sleep;
use tracing::debug;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use types::{
    Certificate, CertificateDigest, CommittedSubDagShell, ConsensusStore, Round, SequenceNumber,
};

use crate::node::Node;

// The number of nodes to spawn.
const NUM_NODES: u16 = 4;
// The amount of time in seconds the nodes are to be running.
const RUNTIME_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    // Start the logger.
    start_logger(LevelFilter::INFO);

    // Create a number of nodes.
    let mut nodes = Vec::with_capacity(NUM_NODES as usize);
    for i in 0..NUM_NODES {
        nodes.push(Node::new(format!("node {}", i), NUM_NODES - 1).await);
    }

    // Connect the nodes in a dense mesh.
    connect_nodes(&nodes, Topology::Mesh).await.unwrap();
    for node in nodes.iter() {
        debug!("node other keys: {:?}", node.other_keys);
    }

    // Create the committee
    let mut authorities = BTreeMap::new();
    for (id, node) in nodes.iter().enumerate() {
        let borrow: &NetworkKeyPair = node.own_network_keypair.borrow();
        let network_key = borrow.public().clone();
        let primary_port = 3000 + id;
        let s = format!("/ip4/127.0.0.1/udp/{}", primary_port);
        let me = Authority {
            stake: 1,
            primary_address: Multiaddr::from_str(s.as_str()).unwrap(),
            network_key,
        };
        let borrow: &BLS12381KeyPair = node.own_keypair.borrow();
        let public = borrow.public().clone();
        authorities.insert(public.clone(), me);
    }
    let committee = Committee {
        authorities,
        epoch: 0,
    };

    debug!("committee: {:?}", &committee);

    let store = make_consensus_store(&Path::new("store"));
    let cert_store = make_certificate_store(&Path::new("cert_store"));
    // Start the consensus for all the nodes.
    for node in nodes.iter_mut() {
        let committee = committee.clone();
        let store = store.clone();
        let cert_store = cert_store.clone();

        let mut clone = node.clone();
        tokio::spawn(async move {
            clone.start_consensus(clone.node.name().to_owned(), committee, store, cert_store)
                .await
        });
    }

    // send certs to node 0
    let genesis = Certificate::genesis(&committee)
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    debug!("genesis: {:?}", &genesis);
    let keys: Vec<_> = committee
        .authorities
        .iter()
        .map(|(a, _)| a.clone())
        .collect();
    debug!("keys: {:?}", keys);
    let (mut certificates, next_parents) =
        test_utils::make_optimal_certificates(&committee, 1..=2, &genesis, &keys);

    // Make two certificate (f+1) with round 3 to trigger the commits.
    let (_, certificate) =
        test_utils::mock_certificate(&committee, keys[0].clone(), 3, next_parents.clone());
    certificates.push_back(certificate);
    let (_, certificate) =
        test_utils::mock_certificate(&committee, keys[1].clone(), 3, next_parents.clone());
    certificates.push_back(certificate);
    let (_, certificate) =
        test_utils::mock_certificate(&committee, keys[2].clone(), 3, next_parents.clone());
    certificates.push_back(certificate);
    debug!("certs created: {:?}", certificates);
    for cert in certificates {
        nodes.get_mut(0).unwrap().inject_cert(cert).await;
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
