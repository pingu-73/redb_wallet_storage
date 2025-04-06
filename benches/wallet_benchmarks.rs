use bdk_chain::bitcoin::Network;
use bdk_wallet::{ChangeSet, KeychainKind, Wallet, WalletPersister};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use redb_wallet_storage::RedbStore;
use tempfile::TempDir;

const EXTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/0/*)";
const INTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/1/*)";

// create test wallet with given no of tx
fn create_test_wallet(tx_count: usize) -> (Wallet, ChangeSet) {
    let mut wallet = Wallet::create(EXTERNAL_DESC, INTERNAL_DESC)
        .network(Network::Signet)
        .create_wallet_no_persist()
        .expect("Failed to create wallet");

    // gen addr to simulate wallet usage
    // collect iterators to address the unused_must_use warnings
    let _ = wallet
        .reveal_addresses_to(KeychainKind::External, tx_count as u32)
        .collect::<Vec<_>>();
    let _ = wallet
        .reveal_addresses_to(KeychainKind::Internal, tx_count as u32)
        .collect::<Vec<_>>();

    // get changeset that would need to be persisted
    let changeset = wallet.take_staged().expect("No staged changes");

    (wallet, changeset)
}

fn bench_wallet_create_and_persist(c: &mut Criterion) {
    let mut group = c.benchmark_group("wallet_create_and_persist");
    group.measurement_time(std::time::Duration::from_secs(15));

    for tx_count in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(tx_count),
            tx_count,
            |b, &tx_count| {
                b.iter_with_setup(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let db_path = temp_dir.path().join(format!("wallet_{}.redb", tx_count));
                        let (_, changeset) = create_test_wallet(tx_count);
                        (temp_dir, db_path, changeset)
                    },
                    |(temp_dir, db_path, changeset)| {
                        let mut store = RedbStore::create(&db_path).unwrap();
                        RedbStore::persist(&mut store, &changeset).unwrap();
                        temp_dir // return to prevent early drop
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_wallet_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("wallet_load");
    group.measurement_time(std::time::Duration::from_secs(23));

    for tx_count in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(tx_count),
            tx_count,
            |b, &tx_count| {
                // new temp dir and database for each iteration
                b.iter_with_setup(
                    || {
                        // Setup: Create a wallet with data in a new temp directory
                        let temp_dir = TempDir::new().unwrap();
                        let db_path = temp_dir.path().join(format!("wallet_{}.redb", tx_count));
                        let (_, changeset) = create_test_wallet(tx_count);

                        // create and populate the database
                        {
                            let mut store = RedbStore::create(&db_path).unwrap();
                            RedbStore::persist(&mut store, &changeset).unwrap();
                            // store drops here and closes the database
                        }

                        // return what we need for the benchmark
                        (temp_dir, db_path)
                    },
                    |(temp_dir, db_path)| {
                        // open the database freshly for each iteration
                        let mut store = RedbStore::open(&db_path).unwrap();

                        let _wallet = Wallet::load()
                            .descriptor(KeychainKind::External, Some(EXTERNAL_DESC))
                            .descriptor(KeychainKind::Internal, Some(INTERNAL_DESC))
                            .load_wallet(&mut store)
                            .expect("Failed to load wallet")
                            .expect("No wallet found");

                        // store drops here and closes the database
                        temp_dir // return to prevent early drop
                    },
                );
            },
        );
    }

    group.finish();
}

// for address derivation and persistence
fn bench_address_derivation(c: &mut Criterion) {
    let mut group = c.benchmark_group("address_derivation");
    group.measurement_time(std::time::Duration::from_secs(8));

    for derivation_count in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(derivation_count),
            derivation_count,
            |b, &derivation_count| {
                b.iter_with_setup(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let db_path = temp_dir
                            .path()
                            .join(format!("wallet_deriv_{}.redb", derivation_count));

                        let mut store = RedbStore::create(&db_path).unwrap();
                        let _wallet = Wallet::create(EXTERNAL_DESC, INTERNAL_DESC)
                            .network(Network::Testnet)
                            .create_wallet(&mut store)
                            .expect("Failed to create wallet");

                        (temp_dir, db_path)
                    },
                    |(temp_dir, db_path)| {
                        let mut store = RedbStore::open(&db_path).unwrap();

                        let mut wallet = Wallet::load()
                            .descriptor(KeychainKind::External, Some(EXTERNAL_DESC))
                            .descriptor(KeychainKind::Internal, Some(INTERNAL_DESC))
                            .load_wallet(&mut store)
                            .expect("Failed to load wallet")
                            .expect("No wallet found");

                        // derive new addresses
                        let _ = wallet
                            .reveal_addresses_to(KeychainKind::External, derivation_count as u32)
                            .collect::<Vec<_>>();

                        // persist the changes
                        wallet.persist(&mut store).unwrap();

                        temp_dir // return to prevent early drop
                    },
                );
            },
        );
    }

    group.finish();
}

// incremental updates (small changes to a large wallet)
fn bench_incremental_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_updates");
    group.measurement_time(std::time::Duration::from_secs(10));

    // create a large wallet in a temp directory for benchmarking
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("large_wallet.redb");

    // set up a large wallet with 1000 addresses
    {
        let (_, initial_changeset) = create_test_wallet(1000);
        let mut store = RedbStore::create(&db_path).unwrap();
        RedbStore::persist(&mut store, &initial_changeset).unwrap();
        // store drops here and close database
    }

    group.bench_function("add_single_address", |b| {
        b.iter_with_setup(
            || {
                // create a copy of database for each iter
                let iter_temp_dir = TempDir::new().unwrap();
                let iter_db_path = iter_temp_dir.path().join("iter_wallet.redb");

                // copy original database to our iter-specific one
                std::fs::copy(&db_path, &iter_db_path).unwrap();

                (iter_temp_dir, iter_db_path)
            },
            |(iter_temp_dir, iter_db_path)| {
                // open the database copy
                let mut store = RedbStore::open(&iter_db_path).unwrap();

                let mut wallet = Wallet::load()
                    .descriptor(KeychainKind::External, Some(EXTERNAL_DESC))
                    .descriptor(KeychainKind::Internal, Some(INTERNAL_DESC))
                    .load_wallet(&mut store)
                    .expect("Failed to load wallet")
                    .expect("No wallet found");

                // derive just one new address
                let current_index = wallet.derivation_index(KeychainKind::External).unwrap_or(0);
                let _ = wallet
                    .reveal_addresses_to(KeychainKind::External, current_index + 1)
                    .collect::<Vec<_>>();

                // persist the change
                wallet.persist(&mut store).unwrap();

                // keep the temp_dir alive until the end of this iteration
                iter_temp_dir
            },
        );
    });

    group.finish();
}

// comparision with file_store
#[cfg(feature = "file_store_comparison")]
fn bench_compare_with_file_store(c: &mut Criterion) {
    use bdk_file_store::Store as FileStore;

    let mut group = c.benchmark_group("storage_comparison");

    for tx_count in [100, 1000].iter() {
        let (_, changeset) = create_test_wallet(*tx_count);

        // benchmark redb
        group.bench_with_input(BenchmarkId::new("redb", tx_count), tx_count, |b, _| {
            b.iter_with_setup(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join("wallet.redb");
                    (temp_dir, db_path, changeset.clone())
                },
                |(temp_dir, db_path, changeset)| {
                    let mut store = RedbStore::create(&db_path).unwrap();
                    RedbStore::persist(&mut store, &changeset).unwrap();
                    temp_dir // Return to prevent early drop
                },
            );
        });

        // Benchmark file_store
        group.bench_with_input(
            BenchmarkId::new("file_store", tx_count),
            tx_count,
            |b, _| {
                b.iter_with_setup(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let file_path = temp_dir.path().join("wallet.dat");
                        (temp_dir, file_path, changeset.clone())
                    },
                    |(temp_dir, file_path, changeset)| {
                        let magic = b"BDK_FILE";
                        let mut store = FileStore::create_new(magic, file_path).unwrap();
                        store.append_changeset(&changeset).unwrap();
                        temp_dir // return to prevent early drop
                    },
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_wallet_create_and_persist,
    bench_wallet_load,
    bench_address_derivation,
    bench_incremental_updates
);

#[cfg(feature = "file_store_comparison")]
criterion_group!(comparison, bench_compare_with_file_store);

#[cfg(feature = "file_store_comparison")]
criterion_main!(benches, comparison);

#[cfg(not(feature = "file_store_comparison"))]
criterion_main!(benches);
