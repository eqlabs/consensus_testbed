## Narwhal/Bullshark test

this starts NUM_NODES (default 5) nodes in pea2pea and also narwhal network on the following ports

* 3000 + i : primary
* 3008 + 2i : worker to worker
* 3009 + 2i : transactions worker

usage: `cargo run`

to inject transactions, use the test_client crate provided:

usage:
```
cd test_client
cargo run <transactionsport> <num>
```