## Narwhal/Bullshark test

this starts NUM_NODES (default 4) nodes in pea2pea and also narwhal network on the following ports

* 3000 + i : primary
* 3008 + 2i : worker to worker
* 3009 + 2i : transactions worker

The narwhal/bullshark config is loaded from JSON files. The config assumes 4 nodes.

usage: 
```
RUST_LOG=info cargo run
```

to inject transactions, use the test_client crate provided:

usage:
```
cd test_client
cargo run <transactionsport> <num>
```