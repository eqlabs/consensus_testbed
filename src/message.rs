use std::io;

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use types::Certificate;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) enum ConsensusMessage {
    CertificateMessage(Vec<Certificate>),
}

#[derive(Default)]
pub(crate) struct ConsensusCodec(LengthDelimitedCodec);

impl Decoder for ConsensusCodec {
    type Item = ConsensusMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.0
            .decode(src)?
            .map(|b| bincode::deserialize(&b).map_err(|_| io::ErrorKind::InvalidData.into()))
            .transpose()
    }
}

impl Encoder<ConsensusMessage> for ConsensusCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ConsensusMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(&item).unwrap().into();
        self.0.encode(bytes, dst)
    }
}
