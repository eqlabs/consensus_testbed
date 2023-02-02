use crate::message::{ConsensusCodec, ConsensusMessage};
use config::{Authority, Committee};
use consensus::{bullshark::Bullshark, metrics::ConsensusMetrics, Consensus};
use crypto::{NetworkKeyPair, PublicKey};
use fastcrypto::bls12381::min_sig::BLS12381KeyPair;
use fastcrypto::{
    hash::Hash,
    traits::{KeyPair, ToFromBytes},
};
use multiaddr::Multiaddr;
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
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    net::SocketAddr,
    path::Path,
    str::FromStr,
    sync::Arc,
};
use storage::CertificateStore;
use store::rocks::ReadWriteOptions;
use store::{
    reopen,
    rocks::{self, DBMap, MetricConf},
};
use tokio::{io::AsyncWriteExt, sync::watch};
use tracing::debug;
use types::{
    metered_channel, Certificate, CertificateDigest, CommittedSubDagShell, ConsensusStore,
    PreSubscribedBroadcastSender, Round, SequenceNumber,
};

#[derive(Clone)]
pub(crate) struct Node {
    node: Pea2PeaNode,
    // consensus
    own_keypair: Arc<BLS12381KeyPair>,
    own_network_keypair: Arc<NetworkKeyPair>,
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
    pub(crate) fn start_consensus(&self) {
        let borrow: &NetworkKeyPair = self.own_network_keypair.borrow();
        let network_key = borrow.public().clone();
        let me = Authority {
            stake: 1,
            primary_address: Multiaddr::from_str("/ip4/127.0.0.1/udp/3000").unwrap(),
            network_key,
        };
        let mut authorities = BTreeMap::new();
        let borrow: &BLS12381KeyPair = self.own_keypair.borrow();
        let public = borrow.public().clone();
        authorities.insert(public.clone(), me);
        let committee = Committee {
            authorities,
            epoch: 0,
        };
        let store = make_consensus_store(&Path::new("store"));
        let cert_store = make_certificate_store(&Path::new("cert_store"));
        let registry = Registry::new();
        let metrics = Arc::new(ConsensusMetrics::new(&registry));
        let protocol = Bullshark::new(committee.clone(), store.clone(), 50, metrics.clone());
        let mut tx_shutdown = PreSubscribedBroadcastSender::new(1);
        let (tx_commited_certificates, rx_commited_certificates) = metered_channel::channel(
            1,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
        let (tx_consensus_round_updates, _rx_consensus_round_updates) = watch::channel(0);
        let (tx_new_certificates, rx_new_certificates) = metered_channel::channel(
            1,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
        let (tx_sequence, rx_sequence) = metered_channel::channel(
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
        tokio::spawn(async move {
            let genesis = Certificate::genesis(&committee)
                .iter()
                .map(|x| x.digest())
                .collect::<BTreeSet<_>>();
            let mut keys: Vec<_> = Vec::new();
            keys.push(public.clone());
            let nodes: Vec<_> = keys.iter().take(3).cloned().collect();
            let mut certificates = VecDeque::new();
            let (out, parents) =
                test_utils::make_optimal_certificates(&committee, 1..=1, &genesis, &nodes);
            certificates.extend(out);
            for cert in certificates {
                tx_new_certificates.send(cert).await;
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
                debug!("{:?}", a);
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
