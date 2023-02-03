use crate::message::{ConsensusCodec, ConsensusMessage};
use config::Committee;
use consensus::{bullshark::Bullshark, metrics::ConsensusMetrics, Consensus};
use crypto::NetworkKeyPair;
use fastcrypto::{
    bls12381::min_sig::BLS12381KeyPair,
    hash::Hash,
    traits::{KeyPair, ToFromBytes},
};
use pea2pea::{
    protocols::{Handshake, Reading, Writing},
    Config, Connection, ConnectionSide, Node as Pea2PeaNode, Pea2Pea,
};
use prometheus::Registry;
use rand::{
    rngs::{OsRng, StdRng},
    SeedableRng,
};
use std::{
    borrow::Borrow,
    collections::{BTreeSet, VecDeque},
    io,
    net::SocketAddr,
    sync::Arc,
};
use storage::CertificateStore;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};
use tracing::{debug, info};
use types::{metered_channel, Certificate, ConsensusStore, PreSubscribedBroadcastSender};

#[derive(Clone)]
pub(crate) struct Node {
    node: Pea2PeaNode,
    // consensus
    pub(crate) own_keypair: Arc<BLS12381KeyPair>,
    pub(crate) own_network_keypair: Arc<NetworkKeyPair>,
    // likely peer keys
}

impl Pea2Pea for Node {
    fn node(&self) -> &Pea2PeaNode {
        &self.node
    }
}

impl Node {
    /// Creates and starts a new node.
    pub(crate) async fn new(name: String, max_connections: u16) -> Self {
        let config = Config {
            name: Some(name),
            max_connections,
            ..Default::default()
        };
        let mut rng = StdRng::from_rng(OsRng).unwrap();
        let kp = KeyPair::generate(&mut rng);
        let own_keypair = Arc::new(kp);
        let own_network_keypair = Arc::new(crypto::traits::KeyPair::generate(&mut rng));

        let node = Self {
            node: Pea2PeaNode::new(config),
            own_keypair,
            own_network_keypair,
        };

        node.enable_handshake().await;
        node.enable_reading().await;
        node.enable_writing().await;
        node.node().start_listening().await.unwrap();

        node
    }

    /// Spawn a task handling the consensus loop.
    pub(crate) fn start_consensus(
        &self,
        committee: Committee,
        store: Arc<ConsensusStore>,
        cert_store: CertificateStore,
    ) {
        let registry = Registry::new();
        let metrics = Arc::new(ConsensusMetrics::new(&registry));
        let protocol = Bullshark::new(committee.clone(), store.clone(), 50, metrics.clone());
        let mut tx_shutdown = PreSubscribedBroadcastSender::new(1);
        let (tx_commited_certificates, mut rx_commited_certificates) = metered_channel::channel(
            100,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
        let (tx_consensus_round_updates, mut _rx_consensus_round_updates) = watch::channel(0);
        let (tx_new_certificates, rx_new_certificates) = metered_channel::channel(
            100,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
        let (tx_sequence, mut rx_sequence) = metered_channel::channel(
            100,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
        let _handle = Consensus::spawn(
            committee.clone(),
            store,
            cert_store,
            tx_shutdown.subscribe(),
            rx_new_certificates,
            tx_commited_certificates,
            tx_consensus_round_updates,
            tx_sequence,
            protocol,
            metrics.clone(),
        );
        let borrow: &BLS12381KeyPair = self.own_keypair.borrow();
        let public = borrow.public().clone();
        tokio::spawn(async move {
            let genesis = Certificate::genesis(&committee)
                .iter()
                .map(|x| x.digest())
                .collect::<BTreeSet<_>>();
            let mut keys: Vec<_> = Vec::new();
            keys.push(public.clone());
            let nodes: Vec<_> = keys.iter().take(3).cloned().collect();
            let mut certificates = VecDeque::new();
            let (out, _parents) =
                test_utils::make_optimal_certificates(&committee, 1..=1, &genesis, &nodes);
            certificates.extend(out);
            debug!("certs created: {:?}", certificates);
            for cert in certificates {
                tx_new_certificates.send(cert).await.ok();
            }
            tokio::select! {
                commited = rx_commited_certificates.recv() => info!("received commited: {:?}", commited),
                sequence = rx_sequence.recv() => info!("received sequence: {:?}", sequence),
            }
        });
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

    async fn process_message(&self, _source: SocketAddr, message: Self::Message) -> io::Result<()> {
        match message {
            ConsensusMessage::CertificateMessage(a) => {
                debug!("received message: {:?}", a);
                Ok(())
            }
        }
    }
}

impl Writing for Node {
    type Message = ConsensusMessage;
    type Codec = ConsensusCodec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}
