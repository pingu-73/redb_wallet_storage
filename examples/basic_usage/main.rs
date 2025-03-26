use bdk_wallet::{bitcoin::Network, CreateParams, KeychainKind, LoadParams, PersistedWallet};
use redb_wallet_storage::RedbStore;
use std::path::Path;

const NETWORK: Network = Network::Testnet;
const EXTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/0/*)";
const INTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/1/*)";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing basic usage of redb_wallet_storage");

    // Open or create a redb store
    let db_path = Path::new("test_wallet.redb");
    println!("Opening or creating store at: {}", db_path.display());
    let mut store = RedbStore::open_or_create(db_path)?;

    // Load existing wallet or create a new one
    let load_params = LoadParams::default();
    let wallet_opt = PersistedWallet::load(&mut store, load_params)?;

    let mut wallet = match wallet_opt {
        Some(wallet) => {
            println!("Loaded existing wallet");
            wallet
        }
        None => {
            println!("Creating new wallet");
            // Create a new wallet with real descriptors
            let create_params = CreateParams::new(EXTERNAL_DESC, INTERNAL_DESC).network(NETWORK);

            PersistedWallet::create(&mut store, create_params)?
        }
    };

    // Generate a new address
    let address = wallet.reveal_next_address(KeychainKind::External);
    println!(
        "Generated new address: {} at index {}",
        address.address, address.index
    );

    // Persist changes
    let persisted = wallet.persist(&mut store)?;
    println!("Changes persisted: {}", persisted);

    // Get wallet info
    println!("Wallet network: {:?}", wallet.network());

    // Close and reopen to verify persistence
    drop(wallet);
    drop(store);

    println!("\nReopening wallet to verify persistence");
    let mut store = RedbStore::open(db_path)?;
    let wallet = PersistedWallet::load(&mut store, LoadParams::default())?.unwrap();

    // Check the address we generated
    let address = wallet.peek_address(KeychainKind::External, 0);
    println!("First address: {}", address.address);

    println!("Basic usage test completed successfully!");

    Ok(())
}
