# Redb Wallet Storage
A redb-based storage backend for Bitcoin Development Kit (BDK) wallets.

## Overview
`redb-wallet-storage` provides a storage backend for Bitcoin Development Kit (BDK) Wallets using [redb](https://github.com/cberner/redb), a pure-Rust embedded key-value store. This implementation offers an alternative to SQLite and file-based storage options.

## Features
- Implements wallet persistence traits for Bitcoin wallet libraries
- Uses redb's transactional key-value storage for reliable data persistence
- Works with both synchronous and asynchronous wallet operations
- Pure Rust implementation

## Installation
Add to you `Cargo.toml`
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
This is a prototype implementation developed as part of a learning project. It may be adapted for use with BDK wallets.
