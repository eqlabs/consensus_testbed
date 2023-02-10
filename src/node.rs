use std::{
    borrow::Borrow,
    collections::HashSet,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use config::{Committee, Parameters, WorkerCache};
use crypto::{NetworkKeyPair, PublicKey};
use executor::ExecutionState;
use fastcrypto::{
    bls12381::min_sig::BLS12381KeyPair,
    traits::{KeyPair, ToFromBytes},
};
use mysten_metrics::RegistryService;
use node::{
    metrics::{primary_metrics_registry, start_prometheus_server, worker_metrics_registry},
    primary_node::PrimaryNode,
    worker_node::WorkerNode,
};
use pea2pea::{
    protocols::{Handshake, Reading, Writing},
    Config, Connection, ConnectionSide, Node as Pea2PeaNode, Pea2Pea,
};
use prometheus::Registry;
use storage::NodeStorage;
use sui_types::crypto::AuthorityKeyPair;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;
use types::ConsensusOutput;
use worker::TrivialTransactionValidator;

use crate::message::{ConsensusCodec, ConsensusMessage};

#[derive(Clone)]
pub(crate) struct Node {
    node: Pea2PeaNode,
    // consensus
    pub(crate) own_keypair: Arc<BLS12381KeyPair>,
    // likely peer keys
    pub(crate) other_keys: Arc<Mutex<HashSet<PublicKey>>>,
}

impl Pea2Pea for Node {
    fn node(&self) -> &Pea2PeaNode {
        &self.node
    }
}

impl Node {
    /// Creates and starts a new node.
    pub(crate) async fn new(
        name: String,
        max_connections: u16,
        primary_keypair: &AuthorityKeyPair,
    ) -> Self {
        let config = Config {
            name: Some(name),
            max_connections,
            ..Default::default()
        };
        let kp = AuthorityKeyPair::from_bytes(primary_keypair.as_bytes()).unwrap();

        let node = Self {
            node: Pea2PeaNode::new(config),
            own_keypair: Arc::new(kp),
            other_keys: Arc::new(Mutex::new(HashSet::new())),
        };

        node.enable_handshake().await;
        node.enable_reading().await;
        node.enable_writing().await;
        node.node().start_listening().await.unwrap();

        node
    }

    /// Spawn a task handling the consensus loop.
    pub(crate) async fn start_consensus(
        &self,
        id: usize,
        primary_keypair: AuthorityKeyPair,
        network_keypair: NetworkKeyPair,
        worker_keypair: NetworkKeyPair,
        parameters: Parameters,
        p_store: NodeStorage,
        w_store: NodeStorage,
        committee: Arc<ArcSwap<Committee>>,
        worker_cache: Arc<ArcSwap<WorkerCache>>,
    ) -> Result<(PrimaryNode, WorkerNode), eyre::Report> {
        let registry_service = RegistryService::new(Registry::new());
        let primary_pub = self.own_keypair.public().clone();
        let primary = PrimaryNode::new(parameters.clone(), true, registry_service);
        primary
            .start(
                primary_keypair,
                network_keypair,
                committee.clone(),
                worker_cache.clone(),
                &p_store,
                Arc::new(MyExecutionState::new(id, self.clone())),
            )
            .await?;
        let prom_address = parameters.clone().prometheus_metrics.socket_addr;
        info!(
            "Starting primary Prometheus HTTP metrics endpoint at {}",
            prom_address
        );
        let registry = primary_metrics_registry(primary_pub.clone());
        let _metrics_server_handle = start_prometheus_server(prom_address.clone(), &registry);

        info!("created primary id {}", id);

        let registry_service = RegistryService::new(Registry::new());
        let worker = WorkerNode::new(0, parameters.clone(), registry_service);
        worker
            .start(
                primary_pub.clone(),
                worker_keypair,
                committee.clone(),
                worker_cache,
                &w_store,
                TrivialTransactionValidator::default(),
                None,
            )
            .await?;
        info!("created worker id {}", id);

        let registry = worker_metrics_registry(id as u32, primary_pub);
        let _metrics_server_handle = start_prometheus_server(prom_address, &registry);

        Ok((primary, worker))
    }

    /// save the ordered transactions
    async fn save_consensus(&self, _consensus_output: ConsensusOutput) {}

    /// TODO: load this from storage
    async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }
}

#[async_trait::async_trait]
impl Handshake for Node {
    async fn perform_handshake(&self, mut conn: Connection) -> io::Result<Connection> {
        let stream = self.borrow_stream(&mut conn);

        let borrow: &BLS12381KeyPair = self.own_keypair.borrow();
        let public = borrow.public().clone();
        let mut src = public.as_bytes();
        stream.write_all_buf(&mut src).await?;

        let mut buf = [0u8; 1024];
        stream.read(&mut buf).await?;
        let other = PublicKey::from_bytes(&buf[..96]).unwrap();
        let others: &Mutex<HashSet<PublicKey>> = self.other_keys.borrow();
        others.lock().unwrap().insert(other);

        Ok(conn)
    }
}

#[async_trait::async_trait]
impl Reading for Node {
    type Message = ConsensusMessage;
    type Codec = ConsensusCodec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }

    async fn process_message(
        &self,
        _source: SocketAddr,
        _message: Self::Message,
    ) -> io::Result<()> {
        // match message {
        //     _ => todo!(),
        // }
        Ok(())
    }
}

impl Writing for Node {
    type Message = ConsensusMessage;
    type Codec = ConsensusCodec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}

pub struct MyExecutionState {
    id: usize,
    node: Node,
}

impl MyExecutionState {
    pub(crate) fn new(id: usize, node: Node) -> Self {
        Self { id, node }
    }
}

#[async_trait]
impl ExecutionState for MyExecutionState {
    /// Receive the consensus result with the ordered transactions in `ConsensusOutupt`
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        info!(
            "Node {} consensus output: {:?} batches",
            self.id,
            consensus_output.batches.len()
        );
        self.node.save_consensus(consensus_output).await;
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        info!("Node {} last_executed_sub_dag_index() called", self.id);
        self.node.last_executed_sub_dag_index().await
    }
}
