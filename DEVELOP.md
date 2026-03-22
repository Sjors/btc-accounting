# Development

## Prerequisites

- Rust toolchain (stable)
- [Cap'n Proto](https://capnproto.org/install.html) compiler (`capnp`)
  - macOS: `brew install capnp`
  - Debian/Ubuntu: `apt install capnproto`
- Bitcoin Core binary (for the integration test)

## Getting Bitcoin Core

The integration test uses Bitcoin Core's IPC interface (multiprocess mode). You
need the `bitcoin` and `bitcoin-node` binaries.

### Option A: Download pre-built binaries (recommended)

Download the release tarball and extract it into the project directory:

```bash
VERSION="31.0rc1"
curl -fsSLO "https://bitcoincore.org/bin/bitcoin-core-${VERSION%%rc*}/test.rc${VERSION##*rc}/bitcoin-${VERSION}-x86_64-linux-gnu.tar.gz"
tar xzf "bitcoin-${VERSION}-x86_64-linux-gnu.tar.gz"
mv "bitcoin-${VERSION}" bitcoin-core
```

On macOS (Apple Silicon):
```bash
VERSION="31.0rc1"
curl -fsSLO "https://bitcoincore.org/bin/bitcoin-core-${VERSION%%rc*}/test.rc${VERSION##*rc}/bitcoin-${VERSION}-aarch64-apple-darwin.tar.gz"
tar xzf "bitcoin-${VERSION}-aarch64-apple-darwin.tar.gz"
mv "bitcoin-${VERSION}" bitcoin-core
```

The test looks for the binary at `bitcoin-core/bin/bitcoin`.

### Option B: Build from source

1. Clone Bitcoin Core into the project directory:
   ```bash
   git clone https://github.com/bitcoin/bitcoin.git bitcoin-core
   ```

2. Configure and build:
   ```bash
   cd bitcoin-core
   cmake -B build -DENABLE_WALLET=ON -DENABLE_IPC=ON -DBUILD_TESTS=OFF -DBUILD_BENCH=OFF
   cmake --build build -j$(nproc) --target bitcoin bitcoin-node
   cd ..
   ```

   On macOS, replace `$(nproc)` with `$(sysctl -n hw.logicalcpu)`.

The test also looks for the binary at `bitcoin-core/build/bin/bitcoin`.

## Running tests

Run unit tests only (no Bitcoin Core needed):
```bash
cargo test --lib
```

Run everything, including the regtest integration test:
```bash
cargo test
```

The integration test (`cargo test --test regtest`) does the following:

1. Starts a Bitcoin Core node in regtest mode with IPC (`-ipcbind=unix`).
2. Creates two deterministic wallets (`mining` and `accounting`) using fixed
   `tprv` keys with `wpkh()` (BIP 84) descriptors.
3. Mines 17 blocks via RPC `generatetoaddress` (avoids the IPC extranonce issue
   at low heights), then 84 blocks via IPC (`createNewBlock` + `submitSolution`)
   for a total of 101 blocks (coinbase maturity).
4. Simulates a 12-month salary scenario: monthly EUR salary → BTC at mock
   exchange rates, with random spending.
5. Exports a CAMT.053 XML statement and verifies it via roundtrip reconstruction.
6. Saves the output to `tests/fixtures/salary_2025_camt053.xml`.

### CAMT.053 schema fixture

The file `tests/fixtures/camt.053.001.02.xsd` is a vendored copy of the
official CAMT.053.001.02 XML Schema used to validate the generated XML fixture.
It is checked into the repository to avoid a network fetch in CI.

CI validates `tests/fixtures/salary_2025_camt053.xml` against that schema with:

```bash
xmllint --schema tests/fixtures/camt.053.001.02.xsd \
  tests/fixtures/salary_2025_camt053.xml --noout
```

### Deterministic blocks

IPC-mined blocks (heights 17+) are produced via Bitcoin Core's Cap'n Proto IPC
interface. The test brute-forces a valid nonce for each block and caches the
coinbase solution (coinbase hex, version, timestamp, nonce) in
`tests/fixtures/coinbase_cache.json`. On subsequent runs, cached solutions are
replayed, producing identical block hashes and transaction IDs. The first 17
blocks are mined via RPC and are not cached.

If the cache is missing or stale (e.g. after changing wallet keys or transaction
amounts), the test regenerates it automatically and emits warnings:

```
⚠️  Cache miss during maturity blocks — output may not be deterministic
```

A cache miss means the current run will produce different block hashes than
previous runs. Delete the cache file and run the test twice to verify
determinism — the XML output should be byte-for-byte identical on the second
run.

### Why `wpkh()` instead of `tr()`

The test uses `wpkh()` (SegWit v0) descriptors rather than `tr()` (taproot)
because Schnorr signatures include randomness by default. This would make
transaction IDs non-deterministic even with identical inputs, defeating the
purpose of the coinbase cache.

### Bitcoin Core extranonce patch

The file `bitcoin-core-ipc-extranonce.patch` patches `src/node/interfaces.cpp`
to set `include_dummy_extranonce = true` when the chain height is below 17.
Without this, `createNewBlock` fails with `bad-cb-length` at early heights
because the BIP 34 height push is only 1 byte, but consensus requires coinbase
scriptSig to be at least 2 bytes.

The integration test avoids this issue by mining the first 17 blocks via RPC
`generatetoaddress` instead of IPC. The patch is only needed if you want to use
IPC mining at heights ≤ 16.
