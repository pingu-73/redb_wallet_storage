use std::{collections::BTreeSet, io::Write};

use bdk_esplora::{esplora_client, EsploraExt};
use bdk_wallet::{
    bitcoin::{Amount, Network},
    CreateParams, KeychainKind, LoadParams, PersistedWallet, SignOptions,
};
use redb_wallet_storage::RedbStore;

const DB_PATH: &str = "bdk-example-redb.db";
const SEND_AMOUNT: Amount = Amount::from_sat(5000);
const STOP_GAP: usize = 5;
const PARALLEL_REQUESTS: usize = 5;

const NETWORK: Network = Network::Signet;
const EXTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/0/*)";
const INTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/1/*)";
const ESPLORA_URL: &str = "http://signet.bitcoindevkit.net";

fn main() -> Result<(), anyhow::Error> {
    let mut store = RedbStore::open_or_create(DB_PATH)?;

    let load_params = LoadParams::default();
    let wallet_opt = PersistedWallet::load(&mut store, load_params)?;

    let mut wallet = match wallet_opt {
        Some(wallet) => {
            println!("Loaded existing wallet from redb store");
            wallet
        }
        None => {
            println!("Creating new wallet with redb store");
            let create_params = CreateParams::new(EXTERNAL_DESC, INTERNAL_DESC).network(NETWORK);
            PersistedWallet::create(&mut store, create_params)?
        }
    };

    let address = wallet.reveal_next_address(KeychainKind::External);
    wallet.persist(&mut store)?;
    println!(
        "Next unused address: ({}) {}",
        address.index, address.address
    );

    let balance = wallet.balance();
    println!("Wallet balance before syncing: {}", balance.total());

    print!("Syncing...");
    let client = esplora_client::Builder::new(ESPLORA_URL).build_blocking();

    let request = wallet.start_full_scan().inspect({
        let mut stdout = std::io::stdout();
        let mut once = BTreeSet::<KeychainKind>::new();
        move |keychain, spk_i, _| {
            if once.insert(keychain) {
                print!("\nScanning keychain [{:?}] ", keychain);
            }
            print!(" {:<3}", spk_i);
            stdout.flush().expect("must flush")
        }
    });

    let update = client.full_scan(request, STOP_GAP, PARALLEL_REQUESTS)?;

    wallet.apply_update(update)?;
    wallet.persist(&mut store)?;
    println!();

    let balance = wallet.balance();
    println!("Wallet balance after syncing: {}", balance.total());

    if balance.total() < SEND_AMOUNT {
        println!(
            "Please send at least {} to the receiving address",
            SEND_AMOUNT
        );
        std::process::exit(0);
    }

    println!("Building transaction to send {} to self...", SEND_AMOUNT);

    let mut tx_builder = wallet.build_tx();
    tx_builder.add_recipient(address.script_pubkey(), SEND_AMOUNT);

    let mut psbt = tx_builder.finish()?;
    let finalized = wallet.sign(&mut psbt, SignOptions::default())?;
    assert!(finalized);

    let tx = psbt.extract_tx()?;
    client.broadcast(&tx)?;
    println!("Tx broadcasted! Txid: {}", tx.compute_txid());

    Ok(())
}
