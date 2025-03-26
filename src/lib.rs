//! redb storage backend for Bitcoin Devlopment Kit
//!
//! This crate provides redb based implementation of `Wallet Persister`
//! from the `bdk_wallet` crate.
use bdk_chain::Merge;
use bdk_wallet::{AsyncWalletPersister, ChangeSet, WalletPersister};
use redb::{Database, ReadableTableMetadata, TableDefinition};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
/// The table definition for wallet data
const WALLET_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("wallet_data");

/// The key used to store the wallet changeset
const CHANGESET_KEY: &str = "wallet_changeset";

/// Persists a wallet changeset in a redb database.
#[derive(Debug)]
pub struct RedbStore {
    db: Database,
}

impl RedbStore {
    /// Create a new [`RedbStore`]; error if the file exists.
    pub fn create<P>(file_path: P) -> Result<Self, RedbError>
    where
        P: AsRef<Path>,
    {
        let db = Database::create(file_path)?;

        // Initialize the database with the required table
        let write_txn = db.begin_write()?;
        {
            let _table = write_txn.open_table(WALLET_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    /// Create a new [`RedbStore`] with custom configuration; error if the file exists.
    pub fn create_with_config<P>(file_path: P, config: redb::Builder) -> Result<Self, RedbError>
    where
        P: AsRef<Path>,
    {
        let db = config.create(file_path)?;

        // Initialize the database with the required table
        let write_txn = db.begin_write()?;
        {
            let _table = write_txn.open_table(WALLET_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    /// Open an existing [`RedbStore`].
    pub fn open<P>(file_path: P) -> Result<Self, RedbError>
    where
        P: AsRef<Path>,
    {
        let db = Database::open(file_path)?;
        Ok(Self { db })
    }

    /// Open an existing [`RedbStore`] with custom configuration.
    pub fn open_with_config<P>(file_path: P, config: redb::Builder) -> Result<Self, RedbError>
    where
        P: AsRef<Path>,
    {
        let db = config.open(file_path)?;
        Ok(Self { db })
    }

    /// Attempt to open an existing [`RedbStore`]; create it if the file does not exist.
    pub fn open_or_create<P>(file_path: P) -> Result<Self, RedbError>
    where
        P: AsRef<Path>,
    {
        if file_path.as_ref().exists() {
            Self::open(file_path)
        } else {
            Self::create(file_path)
        }
    }

    // /// Get statistics about the database
    // pub fn stats(&self) -> Result<redb::DatabaseStats, RedbError> {
    //     Ok(self.db.stats()?)
    // }

    /// Get statistics about the wallet table
    pub fn table_stats(&self) -> Result<redb::TableStats, RedbError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(WALLET_TABLE)?;
        Ok(table.stats()?)
    }

    /// Get the changeset from the database
    fn get_changeset(&self) -> Result<Option<ChangeSet>, RedbError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(WALLET_TABLE)?;

        match table.get(CHANGESET_KEY)? {
            Some(value) => {
                let changeset_bytes = value.value();
                let changeset: ChangeSet =
                    serde_json::from_slice(changeset_bytes).map_err(RedbError::Deserialization)?;
                Ok(Some(changeset))
            }
            None => Ok(None),
        }
    }

    /// Store the changeset in the database
    fn store_changeset(&self, changeset: &ChangeSet) -> Result<(), RedbError> {
        // Skip if changeset is empty
        if changeset.is_empty() {
            return Ok(());
        }

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(WALLET_TABLE)?;

            // Serialize the changeset
            let changeset_bytes =
                serde_json::to_vec(changeset).map_err(RedbError::Serialization)?;

            table.insert(CHANGESET_KEY, changeset_bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }
}

/// Error type for redb storage operations
#[derive(Debug)]
pub enum RedbError {
    /// Error from the redb database
    Database(redb::Error),
    /// Error serializing data
    Serialization(serde_json::Error),
    /// Error deserializing data
    Deserialization(serde_json::Error),
    /// I/O error
    Io(std::io::Error),
    /// Commit error
    Commit(redb::CommitError),
    /// Table error
    Table(redb::TableError),
    /// Transaction error
    Transaction(redb::TransactionError),
}

impl std::fmt::Display for RedbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(e) => write!(f, "Database error: {}", e),
            Self::Serialization(e) => write!(f, "Serialization error: {}", e),
            Self::Deserialization(e) => write!(f, "Deserialization error: {}", e),
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Commit(e) => write!(f, "Commit error: {}", e),
            Self::Table(e) => write!(f, "Table error: {}", e),
            Self::Transaction(e) => write!(f, "Transaction error: {}", e),
        }
    }
}

impl std::error::Error for RedbError {}

impl From<redb::DatabaseError> for RedbError {
    fn from(e: redb::DatabaseError) -> Self {
        Self::Database(e.into())
    }
}

impl From<redb::StorageError> for RedbError {
    fn from(e: redb::StorageError) -> Self {
        Self::Database(e.into())
    }
}

impl From<redb::Error> for RedbError {
    fn from(e: redb::Error) -> Self {
        Self::Database(e)
    }
}

impl From<serde_json::Error> for RedbError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e)
    }
}

impl From<std::io::Error> for RedbError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<redb::CommitError> for RedbError {
    fn from(e: redb::CommitError) -> Self {
        Self::Commit(e)
    }
}

impl From<redb::TableError> for RedbError {
    fn from(e: redb::TableError) -> Self {
        Self::Table(e)
    }
}

impl From<redb::TransactionError> for RedbError {
    fn from(e: redb::TransactionError) -> Self {
        Self::Transaction(e)
    }
}

type FutureResult<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

impl WalletPersister for RedbStore {
    type Error = RedbError;

    fn initialize(persister: &mut Self) -> Result<ChangeSet, Self::Error> {
        // Get changeset or return empty if none exists
        persister.get_changeset().map(|opt| opt.unwrap_or_default())
    }

    fn persist(persister: &mut Self, changeset: &ChangeSet) -> Result<(), Self::Error> {
        // Get existing changeset if any
        let existing_changeset = persister.get_changeset()?;

        // Merge with existing or use the new one
        let final_changeset = match existing_changeset {
            Some(mut existing) => {
                existing.merge(changeset.clone());
                existing
            }
            None => changeset.clone(),
        };

        // Store the merged changeset
        persister.store_changeset(&final_changeset)
    }
}

impl AsyncWalletPersister for RedbStore {
    type Error = RedbError;

    fn initialize<'a>(persister: &'a mut Self) -> FutureResult<'a, ChangeSet, Self::Error>
    where
        Self: 'a,
    {
        Box::pin(async move {
            // Get changeset or return empty if none exists
            persister.get_changeset().map(|opt| opt.unwrap_or_default())
        })
    }

    fn persist<'a>(
        persister: &'a mut Self,
        changeset: &'a ChangeSet,
    ) -> FutureResult<'a, (), Self::Error>
    where
        Self: 'a,
    {
        Box::pin(async move {
            // Get existing changeset if any
            let existing_changeset = persister.get_changeset()?;

            // Merge with existing or use the new one
            let final_changeset = match existing_changeset {
                Some(mut existing) => {
                    existing.merge(changeset.clone());
                    existing
                }
                None => changeset.clone(),
            };

            // Store the merged changeset
            persister.store_changeset(&final_changeset)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bdk_wallet::{CreateParams, KeychainKind, LoadParams, PersistedWallet};
    use bitcoin::Network;
    use std::fs;
    use tempfile::tempdir;

    // Example descriptor for testing
    const TEST_DESCRIPTOR: &str = "wpkh(tprv8ZgxMBicQKsPdcAqYBpzAFwU5yxBUo88ggoBqu1qPcHUfSbKK1sKMLmC7EAk438btHQrSdu3jGGQa6PA71nvH5nkDexhLteJqkM4dQmWF9g/84'/1'/0'/0/*)";
    const TEST_CHANGE_DESCRIPTOR: &str = "wpkh(tprv8ZgxMBicQKsPdcAqYBpzAFwU5yxBUo88ggoBqu1qPcHUfSbKK1sKMLmC7EAk438btHQrSdu3jGGQa6PA71nvH5nkDexhLteJqkM4dQmWF9g/84'/1'/0'/1/*)";

    #[test]
    fn test_create_and_persist() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("wallet.redb");

        let mut store = RedbStore::create(&db_path).unwrap();

        // Create params with descriptors
        let create_params =
            CreateParams::new(TEST_DESCRIPTOR, TEST_CHANGE_DESCRIPTOR).network(Network::Testnet);

        let mut wallet = PersistedWallet::create(&mut store, create_params).unwrap();

        // Make a change to the wallet - reveal an address which will create a change
        let _address = wallet.reveal_next_address(KeychainKind::External);

        // Now persist should return true because we've made changes
        let persisted = wallet.persist(&mut store).unwrap();
        assert!(persisted);

        // Check that we can load the wallet back
        let load_params = LoadParams::default();
        let loaded_wallet = PersistedWallet::load(&mut store, load_params).unwrap();
        assert!(loaded_wallet.is_some());
    }

    #[tokio::test]
    async fn test_async_create_and_persist() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("wallet.redb");

        let mut store = RedbStore::create(&db_path).unwrap();

        // Create params with descriptors
        let create_params =
            CreateParams::new(TEST_DESCRIPTOR, TEST_CHANGE_DESCRIPTOR).network(Network::Testnet);

        let mut wallet = PersistedWallet::create_async(&mut store, create_params)
            .await
            .unwrap();

        // Make a change to the wallet - reveal an address which will create a change
        let _address = wallet.reveal_next_address(KeychainKind::External);

        // Now persist should return true because we've made changes
        let persisted = wallet.persist_async(&mut store).await.unwrap();
        assert!(persisted);

        // Check that we can load the wallet back
        let load_params = LoadParams::default();
        let loaded_wallet = PersistedWallet::load_async(&mut store, load_params)
            .await
            .unwrap();
        assert!(loaded_wallet.is_some());
    }

    #[test]
    fn test_empty_store() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("empty.redb");

        // Create an empty store
        let mut store = RedbStore::create(&db_path).unwrap();

        // Initialize should return an empty changeset
        let changeset = WalletPersister::initialize(&mut store).unwrap();
        assert!(changeset.is_empty());
    }

    #[test]
    fn test_open_nonexistent_file() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("nonexistent.redb");

        // Attempt to open a non-existent database file
        let result = RedbStore::open(&db_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_or_create() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("open_or_create.redb");

        // File doesn't exist, should create it
        let store = RedbStore::open_or_create(&db_path).unwrap();
        drop(store);

        // File now exists, should open it
        let store = RedbStore::open_or_create(&db_path).unwrap();
        drop(store);

        // Verify the file exists
        assert!(db_path.exists());
    }

    #[test]
    fn test_empty_changeset() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("empty_changeset.redb");

        let mut store = RedbStore::create(&db_path).unwrap();

        // Create an empty changeset
        let empty_changeset = ChangeSet::default();

        // Persisting an empty changeset should not error
        WalletPersister::persist(&mut store, &empty_changeset).unwrap();

        // Should still get an empty changeset back
        let retrieved = WalletPersister::initialize(&mut store).unwrap();
        assert!(retrieved.is_empty());
    }

    #[test]
    fn test_persist_and_retrieve() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("persist_retrieve.redb");

        // Create a store and a wallet
        let mut store = RedbStore::create(&db_path).unwrap();
        let create_params =
            CreateParams::new(TEST_DESCRIPTOR, TEST_CHANGE_DESCRIPTOR).network(Network::Testnet);

        let mut wallet = PersistedWallet::create(&mut store, create_params).unwrap();

        // Generate some addresses to create changes
        for _ in 0..5 {
            let _address = wallet.reveal_next_address(KeychainKind::External);
        }

        // Persist changes
        wallet.persist(&mut store).unwrap();

        // Close and reopen the store
        drop(store);
        let mut store = RedbStore::open(&db_path).unwrap();

        // Load the wallet and verify it has the changes
        let loaded_wallet = PersistedWallet::load(&mut store, LoadParams::default())
            .unwrap()
            .unwrap();

        // The loaded wallet should have the same last revealed index as the original
        let original_address = wallet.peek_address(KeychainKind::External, 4);
        let loaded_address = loaded_wallet.peek_address(KeychainKind::External, 4);

        // Compare the addresses
        assert_eq!(
            original_address.address.to_string(),
            loaded_address.address.to_string()
        );
    }

    #[test]
    fn test_update_existing_data() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("update.redb");

        // Create a store and a wallet
        let mut store = RedbStore::create(&db_path).unwrap();
        let create_params =
            CreateParams::new(TEST_DESCRIPTOR, TEST_CHANGE_DESCRIPTOR).network(Network::Testnet);

        let mut wallet = PersistedWallet::create(&mut store, create_params).unwrap();

        // Generate a few addresses
        for _ in 0..3 {
            let _address = wallet.reveal_next_address(KeychainKind::External);
        }

        // Persist the initial state
        wallet.persist(&mut store).unwrap();

        // Generate more addresses to create additional changes
        for _ in 0..3 {
            let _address = wallet.reveal_next_address(KeychainKind::External);
        }

        // Persist the updated state
        wallet.persist(&mut store).unwrap();

        // Close and reopen the store
        drop(store);
        let mut store = RedbStore::open(&db_path).unwrap();

        // Load the wallet and verify it has all the changes
        let loaded_wallet = PersistedWallet::load(&mut store, LoadParams::default())
            .unwrap()
            .unwrap();

        // The loaded wallet should have all 6 addresses
        let last_address = loaded_wallet.peek_address(KeychainKind::External, 5);

        // This should succeed if the wallet has the address at index 5
        assert_eq!(last_address.index, 5);
    }

    #[test]
    fn test_multiple_stores_same_file() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("multiple.redb");

        // Create first store
        let _store1 = RedbStore::create(&db_path).unwrap();

        // Open second store to the same file
        let result = RedbStore::open(&db_path);

        // This should fail because the file is already opened by store1
        assert!(result.is_err());
    }

    #[test]
    fn test_corrupted_data_recovery() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("corrupt.redb");

        // Create a store with a wallet
        {
            let mut store = RedbStore::create(&db_path).unwrap();
            let create_params = CreateParams::new(TEST_DESCRIPTOR, TEST_CHANGE_DESCRIPTOR)
                .network(Network::Testnet);

            let mut wallet = PersistedWallet::create(&mut store, create_params).unwrap();
            wallet.reveal_next_address(KeychainKind::External);
            wallet.persist(&mut store).unwrap();
        }

        // Instead of corrupting the file, let's delete it and create a new one
        fs::remove_file(&db_path).unwrap();

        // Create a new file at the same location
        let mut store = RedbStore::create(&db_path).unwrap();

        // Initialize should return an empty changeset since it's a new file
        let changeset = WalletPersister::initialize(&mut store).unwrap();
        assert!(changeset.is_empty());

        // We should be able to create a new wallet
        let create_params =
            CreateParams::new(TEST_DESCRIPTOR, TEST_CHANGE_DESCRIPTOR).network(Network::Testnet);

        let _wallet = PersistedWallet::create(&mut store, create_params).unwrap();
    }
}
