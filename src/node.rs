use crate::message::{ConsensusCodec, ConsensusMessage};
use pea2pea::{
    protocols::{Handshake, Reading, Writing},
    Config, Connection, ConnectionSide, Node as Pea2PeaNode, Pea2Pea,
};
use std::{io, net::SocketAddr};
use types::ConsensusOutput;

#[derive(Clone)]
pub(crate) struct Node {
    node: Pea2PeaNode,
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

    // TODO: move the below 2 functions to a trait?

    /// save the ordered transactions
    pub(crate) async fn save_consensus(&self, _consensus_output: ConsensusOutput) {}

    /// TODO: load this from storage
    pub(crate) async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }
}

#[async_trait::async_trait]
impl Handshake for Node {
    async fn perform_handshake(&self, conn: Connection) -> io::Result<Connection> {
        // let stream = self.borrow_stream(&mut conn);

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
