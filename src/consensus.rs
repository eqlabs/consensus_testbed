use crate::node::Node;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use config::{Committee, Import, Parameters, WorkerCache};
use crypto::NetworkKeyPair;
use executor::ExecutionState;
use eyre::Context;
use fastcrypto::traits::KeyPair;
use mysten_metrics::RegistryService;
use node::{
    metrics::{primary_metrics_registry, start_prometheus_server, worker_metrics_registry},
    primary_node::PrimaryNode,
    worker_node::WorkerNode,
    NodeStorage,
};
use prometheus::Registry;
use std::sync::Arc;
use sui_keys::keypair_file::{read_authority_keypair_from_file, read_network_keypair_from_file};
use sui_types::crypto::AuthorityKeyPair;
use tracing::{debug, info};
use types::ConsensusOutput;
use worker::TrivialTransactionValidator;

pub(crate) struct Consensus {
    id: u32,
    primary_keypair: AuthorityKeyPair,
    network_keypair: NetworkKeyPair,
    worker_keypair: NetworkKeyPair,
    parameters: Parameters,
    p_store: NodeStorage,
    w_store: NodeStorage,
    committee: Arc<ArcSwap<Committee>>,
    worker_cache: Arc<ArcSwap<WorkerCache>>,
}

impl Consensus {
    pub(crate) fn new(id: u32) -> Result<Self, eyre::Report> {
        let primary_key_file = format!(".primary-{id}-key.json");
        let primary_keypair = read_authority_keypair_from_file(primary_key_file)
            .expect("Failed to load the node's primary keypair");
        let primary_network_key_file = format!(".primary-{id}-network-key.json");
        let network_keypair = read_network_keypair_from_file(primary_network_key_file)
            .expect("Failed to load the node's primary network keypair");
        let worker_key_file = format!(".worker-{id}-key.json");
        let worker_keypair = read_network_keypair_from_file(worker_key_file)
            .expect("Failed to load the node's worker keypair");
        debug!("creating task {}", id);
        // Read the committee, workers and node's keypair from file.
        let committee_file = ".committee.json";
        let committee = Arc::new(ArcSwap::from_pointee(
            Committee::import(committee_file)
                .context("Failed to load the committee information")?,
        ));
        let workers_file = ".workers.json";
        let worker_cache = Arc::new(ArcSwap::from_pointee(
            WorkerCache::import(workers_file).context("Failed to load the worker information")?,
        ));

        // Load default parameters if none are specified.
        let filename = ".parameters.json";
        let parameters =
            Parameters::import(filename).context("Failed to load the node's parameters")?;

        // Make the data store.
        let store_path = format!(".db-{id}-key.json");
        let p_store = NodeStorage::reopen(store_path);
        let store_path = format!(".db-{id}-0-key.json");
        let w_store = NodeStorage::reopen(store_path);
        Ok(Self {
            id,
            primary_keypair,
            network_keypair,
            worker_keypair,
            parameters,
            p_store,
            w_store,
            committee,
            worker_cache,
        })
    }

    /// Start the primary and worker node
    /// only 1 worker is spawned ATM
    /// caller must call `wait().await` on primary and worker
    pub(crate) async fn start(
        self,
        node: &Node,
    ) -> Result<(PrimaryNode, WorkerNode), eyre::Report> {
        let registry_service = RegistryService::new(Registry::new());
        let primary_pub = self.primary_keypair.public().clone();
        let primary = PrimaryNode::new(self.parameters.clone(), true, registry_service);
        primary
            .start(
                self.primary_keypair,
                self.network_keypair,
                self.committee.clone(),
                self.worker_cache.clone(),
                &self.p_store,
                Arc::new(MyExecutionState::new(self.id, node.clone())),
            )
            .await?;
        let prom_address = self.parameters.clone().prometheus_metrics.socket_addr;
        info!(
            "Starting primary Prometheus HTTP metrics endpoint at {}",
            prom_address
        );
        let registry = primary_metrics_registry(primary_pub.clone());
        let _metrics_server_handle = start_prometheus_server(prom_address.clone(), &registry);

        info!("created primary id {}", self.id);

        let registry_service = RegistryService::new(Registry::new());
        let worker = WorkerNode::new(0, self.parameters.clone(), registry_service);
        worker
            .start(
                primary_pub.clone(),
                self.worker_keypair,
                self.committee.clone(),
                self.worker_cache,
                &self.w_store,
                TrivialTransactionValidator::default(),
                None,
            )
            .await?;
        info!("created worker id {}", self.id);

        let registry = worker_metrics_registry(self.id, primary_pub);
        let _metrics_server_handle = start_prometheus_server(prom_address, &registry);
        Ok((primary, worker))
    }
}

pub struct MyExecutionState {
    id: u32,
    node: Node,
}

impl MyExecutionState {
    pub(crate) fn new(id: u32, node: Node) -> Self {
        Self { id, node }
    }
}

#[async_trait]
impl ExecutionState for MyExecutionState {
    /// Receive the consensus result with the ordered transactions in `ConsensusOutupt`
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        if !consensus_output.batches.is_empty() {
            info!(
                "Node {} consensus output for round {}: {:?} batches, leader: {:?}",
                self.id,
                consensus_output.sub_dag.leader.header.round,
                consensus_output.batches.len(),
                consensus_output.sub_dag.leader.header.author,
            );
            self.node.save_consensus(consensus_output).await;
        }
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        info!("Node {} last_executed_sub_dag_index() called", self.id);
        self.node.last_executed_sub_dag_index().await
    }
}
