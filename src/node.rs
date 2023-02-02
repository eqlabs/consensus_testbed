use std::{io, net::SocketAddr};

use pea2pea::{
    protocols::{Handshake, Reading, Writing},
    Config, Connection, ConnectionSide, Node as Pea2PeaNode, Pea2Pea,
};

use crate::message::{ConsensusCodec, ConsensusMessage};

#[derive(Clone)]
pub(crate) struct Node {
    node: Pea2PeaNode,
    // consensus
    // likely own key
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

        let node = Self {
            node: Pea2PeaNode::new(config),
        };

        node.enable_handshake().await;
        node.enable_reading().await;
        node.enable_writing().await;
        node.node().start_listening().await.unwrap();

        node
    }

    /// Spawn a task handling the consensus loop.
    pub(crate) fn start_consensus(&self) {
        let node = self.clone();
        tokio::spawn(async move { loop {} });
    }
}

#[async_trait::async_trait]
impl Handshake for Node {
    async fn perform_handshake(&self, mut conn: Connection) -> io::Result<Connection> {
        let stream = self.borrow_stream(&mut conn);

        /* exchange any keys */

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
            _ => todo!(),
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
