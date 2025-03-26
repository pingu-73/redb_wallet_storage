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
or 
```
[dependencies]
redb_wallet_storage = { git = "https://github.com/pingu-73/redb_wallet_storage" }
```

## Example
See the [examples](https://github.com/pingu-73/redb_wallet_storage/tree/main/examples/) directory 

## Project Status
This is a prototype implementation developed as part of a learning project. It may be adapted for use with BDK wallets.