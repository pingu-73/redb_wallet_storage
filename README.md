# Redb Wallet Storage
**A redb-based storage backend for [Bitcoin Development Kit (BDK)](https://bitcoindevkit.org/) wallets.**
<div align="center">
  <p>
<!--     <a href="https://crates.io/crates/redb_wallet_storage"><img alt="Crate Info" src="https://img.shields.io/crates/v/bdk_wallet.svg"/></a> -->
<!--     <a href="https://github.com/pingu-73/redb_wallet_storage/blob/main/LICENSE"><img alt="MIT or Apache-2.0 Licensed" src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg"/></a> -->
  </p>

  <h4>
    <a href="https://docs.rs/redb_wallet_storage">Documentation</a>
    <span> | </span>
    <a href="https://docs.rs/redb_wallet_storage">Crates.io</a>
    <span> | </span>
    <a href="https://github.com/pingu-73/redb_wallet_storage/blob/main/LICENSE">MIT or Apache-2.0 Licensed</a>

  </h4>
</div>

## Overview
`redb-wallet-storage` provides a storage backend for [Bitcoin Development Kit (BDK)](https://bitcoindevkit.org/) Wallets using [redb](https://github.com/cberner/redb), a pure-Rust embedded key-value store. This implementation offers an alternative to SQLite and file-based storage options.

## Features
- Implements wallet persistence traits for Bitcoin wallet libraries
- Uses redb's transactional key-value storage for reliable data persistence
- Works with both synchronous and asynchronous wallet operations
- Pure Rust implementation

## Installation
Add to your `Cargo.toml`
```
[dependencies]
redb_wallet_storage = "0.1.0"
```
or use development version
```
[dependencies]
redb_wallet_storage = { git = "https://github.com/pingu-73/redb_wallet_storage" }
```

## Usage
```
use bdk_wallet::{CreateParams, LoadParams, PersistedWallet, KeychainKind};
use bitcoin::Network;
use redb_wallet_storage::RedbStore;

// Create or open a wallet store
let mut store = RedbStore::open_or_create("wallet.redb")?;

// Try to load an existing wallet
let mut wallet = match PersistedWallet::load(&mut store, LoadParams::default())? {
    Some(wallet) => {
        println!("Loaded existing wallet");
        wallet
    },
    None => {
        // Create a new wallet if none exists
        println!("Creating new wallet");
        let descriptor = "wpkh(...)"; // Your descriptor here
        let change_descriptor = "wpkh(...)"; // Your change descriptor here
        
        let create_params = CreateParams::new(descriptor, change_descriptor)
            .network(Network::Testnet);
            
        PersistedWallet::create(&mut store, create_params)?
    }
};

// Generate a new address
let address = wallet.reveal_next_address(KeychainKind::External);
println!("New address: {}", address.address);

// Persist changes to the database
wallet.persist(&mut store)?;
```

### Async Usage
```
use bdk_wallet::{CreateParams, LoadParams, PersistedWallet, KeychainKind};
use bitcoin::Network;
use redb_wallet_storage::RedbStore;

async fn wallet_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create or open a wallet store
    let mut store = RedbStore::open_or_create("wallet_async.redb")?;
    
    // Load or create wallet asynchronously
    let mut wallet = match PersistedWallet::load_async(&mut store, LoadParams::default()).await? {
        Some(wallet) => wallet,
        None => {
            let descriptor = "wpkh(...)"; // Your descriptor here
            let change_descriptor = "wpkh(...)"; // Your change descriptor here
            
            let create_params = CreateParams::new(descriptor, change_descriptor)
                .network(Network::Testnet);
                
            PersistedWallet::create_async(&mut store, create_params).await?
        }
    };
    
    // Generate a new address
    let address = wallet.reveal_next_address(KeychainKind::External);
    
    // Persist changes asynchronously
    wallet.persist_async(&mut store).await?;
    
    Ok(())
}
```

## Example
See the [examples](https://github.com/pingu-73/redb_wallet_storage/tree/main/examples/) directory 

## Project Status
This is a prototype implementation developed as part of a learning project.

## Contribution
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
