use bytes::BufMut;
use futures_util::stream::StreamExt;
use multiaddr::Multiaddr;
use rand::Rng;
use std::error::Error;
use std::str::FromStr;
use tokio::time::interval;
use tokio::time::Duration;
use tracing::{info, warn};
use types::{TransactionProto, TransactionsClient};

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let mut args: Vec<String> = std::env::args().collect();
    let port = args.remove(1);
    let num = args.remove(1).parse().unwrap();
    let s = format!("/ip4/0.0.0.0/tcp/{}/http", port);
    let address = Multiaddr::from_str(s.as_str()).unwrap();
    let config = mysten_network::config::Config::new();
    let channel = config.connect_lazy(&address).unwrap();
    let mut client = TransactionsClient::new(channel);
    // let payload = bytes::Bytes::from(lipsum::lipsum(200));
    // let tx = TransactionProto {
    //     transaction: payload,
    // };
    // for _ in 1..=num {
    //     client.submit_transaction(tx.clone()).await?;
    // }

    // We are distributing the transactions that need to be sent
    // within a second to sub-buckets. The precision here represents
    // the number of such buckets within the period of 1 second.
    const PRECISION: u64 = 20;
    // The BURST_DURATION represents the period for each bucket we
    // have split. For example if precision is 20 the 1 second (1000ms)
    // will be split in 20 buckets where each one will be 50ms apart.
    // Basically we are looking to send a list of transactions every 50ms.
    const BURST_DURATION: u64 = 1000 / PRECISION;

    let mut counter = 0;
    let rate = 500;
    let burst = rate / PRECISION;
    let size = 512;
    let mut r = rand::thread_rng().gen();
    let interval = interval(Duration::from_millis(BURST_DURATION));
    tokio::pin!(interval);
    'main: loop {
        interval.as_mut().tick().await;
        let mut tx = bytes::BytesMut::with_capacity(512);
        let stream = tokio_stream::iter(0..burst).map(move |x| {
            if x == counter % burst {
                // NOTE: This log entry is used to compute performance.
                info!("Sending sample transaction {counter}");

                tx.put_u8(0u8); // Sample txs start with 0.
                tx.put_u64(counter); // This counter identifies the tx.
            } else {
                r += 1;
                tx.put_u8(1u8); // Standard txs start with 1.
                tx.put_u64(r); // Ensures all clients send different txs.
            };

            tx.resize(size, 0u8);
            let bytes = tx.split().freeze();
            TransactionProto { transaction: bytes }
        });

        if let Err(e) = client.submit_transaction_stream(stream).await {
            warn!("Failed to send transaction: {e}");
            break 'main;
        }

        counter += 1;
        if counter > num {
            break;
        }
    }
    Ok(())
}
