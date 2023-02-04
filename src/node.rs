use crate::message::{ConsensusCodec, ConsensusMessage};
use config::Committee;
use consensus::{bullshark::Bullshark, metrics::ConsensusMetrics, Consensus};
use crypto::{NetworkKeyPair, PublicKey};
use fastcrypto::{
    bls12381::min_sig::BLS12381KeyPair,
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
    borrow::{Borrow},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex}, collections::HashSet,
};
use storage::CertificateStore;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};
use tracing::debug;
use types::{
    metered_channel::{self, Sender},
    Certificate, ConsensusStore, PreSubscribedBroadcastSender,
};

#[derive(Clone)]
pub(crate) struct Node {
    pub(crate) node: Pea2PeaNode,
    // consensus
    pub(crate) own_keypair: Arc<BLS12381KeyPair>,
    pub(crate) own_network_keypair: Arc<NetworkKeyPair>,
    // likely peer keys
    pub(crate) other_keys: Arc<Mutex<HashSet<PublicKey>>>,

    // channels
    tx_new_certificates: Option<Arc<Sender<Certificate>>>,
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
            other_keys: Arc::new(Mutex::new(HashSet::new())),
            tx_new_certificates: None,
        };

        node.enable_handshake().await;
        node.enable_reading().await;
        node.enable_writing().await;
        node.node().start_listening().await.unwrap();

        node
    }

    /// Spawn a task handling the consensus loop.
    pub(crate) async fn start_consensus(
        &mut self,
        name: String,
        committee: Committee,
        store: Arc<ConsensusStore>,
        cert_store: CertificateStore,
    ) {
        let registry = Registry::new();
        let metrics = Arc::new(ConsensusMetrics::new(&registry));
        let protocol = Bullshark::new(committee.clone(), store.clone(), 50, metrics.clone());
        let mut tx_shutdown = PreSubscribedBroadcastSender::new(25);
        let (tx_commited_certificates, mut rx_commited_certificates) = metered_channel::channel(
            1,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
        let (tx_consensus_round_updates, mut _rx_consensus_round_updates) = watch::channel(0);
        let (tx_new_certificates, rx_new_certificates) = metered_channel::channel(
            1,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
        self.tx_new_certificates = Some(Arc::new(tx_new_certificates));
        let (tx_sequence, mut rx_sequence) = metered_channel::channel(
            1,
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
        let name_clone = name.clone();
        loop {
            tokio::select! {
                Some(c) = rx_commited_certificates.recv() => {
                    debug!("{} commited: {:?}", name_clone, c);
                    self.broadcast(ConsensusMessage::CommittedCertificateMessage(c.1)).ok();
                },
                Some(c) = rx_sequence.recv() => {
                    debug!("{} commited DAG: {:?}", name_clone, c);
                    self.broadcast(ConsensusMessage::CommittedDagMessage(c)).ok();
                }
                // Some(c) = self.rx_consensus_round_updates.borrow_mut().borrow() => {
                //     debug!("{} round update: {:?}", name_clone, c);
                //     self.broadcast(ConsensusMessage::RoundUpdateMessage(c)).ok();
                // }
                else => {
                    break;
                }
            }
        }
        debug!("handle: {:?}", _handle.await);
    }

    pub(crate) async fn inject_cert(&self, cert: Certificate) {
        self.tx_new_certificates.borrow().as_ref().unwrap().send(cert).await.ok();
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

    async fn process_message(&self, _source: SocketAddr, message: Self::Message) -> io::Result<()> {
        match message {
            ConsensusMessage::CommittedCertificateMessage(a) => {
                debug!(
                    "{} received commited certs message: {:?}",
                    self.node.name(),
                    a
                );
                Ok(())
            }
            ConsensusMessage::CertificateMessage(certs) => {
                debug!("{} received certs message: {:?}", self.node.name(), certs);
                for c in certs {
                    let send = self.tx_new_certificates.borrow().as_ref().unwrap().send(c);
                    send.await.ok();
                }
                Ok(())
            }
            ConsensusMessage::CommittedDagMessage(dag) => {
                debug!("{} received dag message: {:?}", self.node.name(), dag);
                // TODO: what to do here??
                Ok(())
            }
            ConsensusMessage::RoundUpdateMessage(round) => {
                debug!("{} received roundupdate message: {:?}", self.node.name(), round);
                // TODO: what to do here??
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
