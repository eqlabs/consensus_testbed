mod message;
mod node;

use ::node::NodeStorage;
use arc_swap::ArcSwap;
use config::{
    AnemoParameters, Authority, Committee, NetworkAdminServerParameters, Parameters,
    PrometheusMetricsParameters, WorkerCache, WorkerId, WorkerIndex, WorkerInfo,
};
use crypto::PublicKey;
use fastcrypto::{bls12381::min_sig::BLS12381KeyPair, ed25519::Ed25519KeyPair, traits::KeyPair};
use multiaddr::Multiaddr;
use pea2pea::{connect_nodes, Topology};
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
use sui_types::crypto::{get_key_pair_from_rng, AuthorityKeyPair, NetworkKeyPair};
use tokio::time::sleep;
use tracing::debug;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use crate::node::Node;

// The number of nodes to spawn.
const NUM_NODES: u16 = 2;
// The amount of time in seconds the nodes are to be running.
const RUNTIME_SECS: u64 = 60;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    // Start the logger.
    start_logger(LevelFilter::INFO);

    // parameters
    let block_sync_params = config::BlockSynchronizerParameters::default();
    let consensus_api_grpc = config::ConsensusAPIGrpcParameters {
        socket_addr: Multiaddr::from_str("/ip4/0.0.0.0/tcp/0/http").unwrap(),
        get_collections_timeout: Duration::from_secs(5000),
        remove_collections_timeout: Duration::from_secs(5000),
    };
    let prometheus_metrics = PrometheusMetricsParameters {
        socket_addr: Multiaddr::from_str("/ip4/0.0.0.0/tcp/0/http").unwrap(),
    };
    let network_admin_server = NetworkAdminServerParameters {
        primary_network_admin_server_port: 0,
        worker_network_admin_server_base_port: 0,
    };
    let anemo = AnemoParameters::default();
    let parameters = Parameters {
        header_num_of_batches_threshold: 32,
        min_header_delay: Duration::from_millis(100),
        max_header_num_of_batches: 1000,
        max_header_delay: Duration::from_secs(200),
        gc_depth: 10,
        sync_retry_delay: Duration::from_secs(10),
        sync_retry_nodes: 3,
        batch_size: 500000,
        max_batch_delay: Duration::from_millis(200),
        block_synchronizer: block_sync_params,
        consensus_api_grpc,
        max_concurrent_requests: 500000,
        prometheus_metrics,
        network_admin_server,
        anemo,
    };
    debug!("parameters: {:?}", parameters);

    // Create the committee
    let mut primary_keys = Vec::new();
    for _ in 0..NUM_NODES {
        let primary_keypair: AuthorityKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
        primary_keys.push(primary_keypair);
    }
    let mut network_keys = Vec::new();
    for _ in 0..NUM_NODES {
        let network_keypair: NetworkKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
        network_keys.push(network_keypair);
    }
    let mut worker_keys = Vec::new();
    for _ in 0..NUM_NODES {
        let worker_keypair: NetworkKeyPair = get_key_pair_from_rng(&mut rand::rngs::OsRng).1;
        worker_keys.push(worker_keypair);
    }

    // create the committee
    let c = create_committee(&primary_keys, &network_keys);
    debug!("committee : {:?}", c);
    let committee = Arc::new(ArcSwap::from_pointee(c));

    // create the worker cache
    let w = create_worker_cache(&primary_keys, &worker_keys);
    debug!("worker cache: {:?}", w);
    let worker_cache = Arc::new(ArcSwap::from_pointee(w));

    debug!("committee: {:?}", &committee);

    // Create a number of nodes.
    let mut nodes = Vec::with_capacity(NUM_NODES as usize);
    for i in 0..NUM_NODES as usize {
        nodes.push(
            Node::new(
                "node_name".to_string(),
                NUM_NODES - 1,
                primary_keys.get(i).unwrap(),
            )
            .await,
        );
    }

    // Connect the nodes in a dense mesh.
    connect_nodes(&nodes, Topology::Mesh).await.unwrap();

    // Start the consensus for all the nodes.
    let store_path = "store";
    let store = NodeStorage::reopen(store_path);
    let mut handles = Vec::new();
    for (i, node) in nodes.into_iter().enumerate() {
        let committee = committee.clone();
        let store = store.clone();
        let p = parameters.clone();
        let pk = primary_keys.remove(0);
        let nk = network_keys.remove(0);
        let wk = worker_keys.remove(0);
        let s = store.clone();
        let c = committee.clone();
        let wc = worker_cache.clone();
        let h = tokio::spawn(async move {
            let (primary, worker) = node.start_consensus(i, pk, nk, wk, p, s, c, wc).await.unwrap();
            primary.wait().await;
            worker.wait().await;
        });
        handles.push(h);
    }

    // Wait for a desired amount of time to allow the nodes to do some work.
    sleep(Duration::from_secs(RUNTIME_SECS)).await;
    for h in handles {
        h.await?;
    }
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
        .without_time()
        .with_target(false)
        .init();
}

/// create the `Committee` up front from the keys
fn create_committee(
    primary_keys: &[BLS12381KeyPair],
    network_keys: &[Ed25519KeyPair],
) -> Committee {
    let mut authorities = BTreeMap::<PublicKey, Authority>::new();
    let start_port = 3000usize;
    for i in 0..NUM_NODES as usize {
        let addr = format!("/ip4/127.0.0.1/udp/{}", start_port + i);
        authorities.insert(
            primary_keys.get(i).unwrap().public().clone(),
            Authority {
                stake: 1000,
                primary_address: Multiaddr::from_str(addr.as_str()).unwrap(),
                network_key: network_keys.get(i).unwrap().public().clone(),
            },
        );
    }
    Committee {
        authorities,
        epoch: 0,
    }
}

/// create the `WorkerCache` up front from the keys
fn create_worker_cache(
    primary_keys: &[BLS12381KeyPair],
    worker_keys: &[Ed25519KeyPair],
) -> WorkerCache {
    let mut workers = BTreeMap::<PublicKey, WorkerIndex>::new();
    let mut port = 3008usize;
    for i in 0..NUM_NODES as usize {
        let worker = format!("/ip4/127.0.0.1/udp/{port}");
        port += 1;
        let transactions = format!("/ip4/127.0.0.1/tcp/{port}/http");
        port += 1;
        let mut worker_info = BTreeMap::<WorkerId, WorkerInfo>::new();
        worker_info.insert(
            0,
            WorkerInfo {
                name: worker_keys.get(i).unwrap().public().clone(),
                transactions: Multiaddr::from_str(transactions.as_str()).unwrap(),
                worker_address: Multiaddr::from_str(worker.as_str()).unwrap(),
            },
        );
        let worker_index = WorkerIndex(worker_info);
        workers.insert(primary_keys.get(i).unwrap().public().clone(), worker_index);
    }
    WorkerCache { workers, epoch: 0 }
}
