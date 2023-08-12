#![allow(dead_code)]

pub mod byte_block;
pub mod error;
pub mod file;
pub mod footer;
pub mod hot;
pub mod index;
pub mod meta;
pub mod mmap_utils;
pub mod readable;
pub mod writer;

use {
    crate::{
        account_storage::meta::{StorableAccountsWithHashesAndWriteVersions, StoredAccountInfo},
        storable_accounts::StorableAccounts,
    },
    error::TieredStorageError,
    footer::{AccountBlockFormat, AccountMetaFormat, OwnersBlockFormat},
    index::AccountIndexFormat,
    once_cell::sync::OnceCell,
    readable::TieredStorageReader,
    solana_sdk::{account::ReadableAccount, hash::Hash},
    std::{
        borrow::Borrow,
        fs::OpenOptions,
        path::{Path, PathBuf},
    },
    writer::TieredStorageWriter,
};

pub type TieredStorageResult<T> = Result<T, TieredStorageError>;

/// The struct that defines the formats of all building blocks of a
/// TieredStorage.
#[derive(Clone, Debug)]
pub struct TieredStorageFormat {
    pub meta_entry_size: usize,
    pub account_meta_format: AccountMetaFormat,
    pub owners_block_format: OwnersBlockFormat,
    pub account_index_format: AccountIndexFormat,
    pub account_block_format: AccountBlockFormat,
}

#[derive(Debug)]
pub struct TieredStorage {
    reader: OnceCell<TieredStorageReader>,
    format: Option<TieredStorageFormat>,
    path: PathBuf,
}

impl Drop for TieredStorage {
    fn drop(&mut self) {
        if let Err(err) = fs_err::remove_file(&self.path) {
            panic!("TieredStorage failed to remove backing storage file: {err}");
        }
    }
}

impl TieredStorage {
    /// Creates a new writable instance of TieredStorage based on the
    /// specified path and TieredStorageFormat.
    ///
    /// Note that the actual file will not be created until write_accounts
    /// is called.
    pub fn new_writable(path: impl Into<PathBuf>, format: TieredStorageFormat) -> Self {
        Self {
            reader: OnceCell::<TieredStorageReader>::new(),
            format: Some(format),
            path: path.into(),
        }
    }

    /// Creates a new read-only instance of TieredStorage from the
    /// specified path.
    pub fn new_readonly(path: impl Into<PathBuf>) -> TieredStorageResult<Self> {
        let path = path.into();
        Ok(Self {
            reader: OnceCell::with_value(TieredStorageReader::new_from_path(&path)?),
            format: None,
            path,
        })
    }

    /// Returns the path to this TieredStorage.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Writes the specified accounts into this TieredStorage.
    ///
    /// Note that this function can only be called once per a TieredStorage
    /// instance.  TieredStorageError::AttemptToUpdateReadOnly will be returned
    /// if this function is invoked more than once on the same TieredStorage
    /// instance.
    pub fn write_accounts<
        'a,
        'b,
        T: ReadableAccount + Sync,
        U: StorableAccounts<'a, T>,
        V: Borrow<Hash>,
    >(
        &self,
        accounts: &StorableAccountsWithHashesAndWriteVersions<'a, 'b, T, U, V>,
        skip: usize,
    ) -> TieredStorageResult<Vec<StoredAccountInfo>> {
        if self.is_read_only() {
            return Err(TieredStorageError::AttemptToUpdateReadOnly(
                self.path.to_path_buf(),
            ));
        }

        let result = {
            // self.format must be Some as write_accounts can only be called on a
            // TieredStorage instance created via new_writable() where its format
            // field is required.
            let writer = TieredStorageWriter::new(&self.path, self.format.as_ref().unwrap())?;
            writer.write_accounts(accounts, skip)
        };

        // panic here if self.reader.get() is not None as self.reader can only be
        // None since we have passed `is_read_only()` check previously, indicating
        // self.reader is not yet set.
        self.reader
            .set(TieredStorageReader::new_from_path(&self.path)?)
            .unwrap();

        result
    }

    /// Returns the underlying reader of the TieredStorage.  None will be
    /// returned if it's is_read_only() returns false.
    pub fn reader(&self) -> Option<&TieredStorageReader> {
        self.reader.get()
    }

    /// Returns true if the TieredStorage instance is read-only.
    pub fn is_read_only(&self) -> bool {
        self.reader.get().is_some()
    }

    /// Returns the size of the underlying accounts file.
    pub fn file_size(&self) -> TieredStorageResult<u64> {
        let file = OpenOptions::new().read(true).open(&self.path);

        Ok(file
            .and_then(|file| file.metadata())
            .map(|metadata| metadata.len())
            .unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::account_storage::meta::{StoredMeta, StoredMetaWriteVersion},
        footer::{TieredStorageFooter, TieredStorageMagicNumber},
        hot::{HotAccountMeta, HOT_FORMAT},
        solana_sdk::{
            account::{Account, AccountSharedData},
            clock::Slot,
            pubkey::Pubkey,
        },
        std::mem::ManuallyDrop,
        tempfile::tempdir,
    };

    impl TieredStorage {
        fn footer(&self) -> Option<&TieredStorageFooter> {
            self.reader.get().map(|r| r.footer())
        }
    }

    /// Simply invoke write_accounts with empty vector to allow the tiered storage
    /// to persist non-account blocks such as footer, index block, etc.
    fn write_zero_accounts(
        tiered_storage: &TieredStorage,
        expected_result: TieredStorageResult<Vec<StoredAccountInfo>>,
    ) {
        let slot_ignored = Slot::MAX;
        let account_refs = Vec::<(&Pubkey, &AccountSharedData)>::new();
        let account_data = (slot_ignored, account_refs.as_slice());
        let storable_accounts =
            StorableAccountsWithHashesAndWriteVersions::new_with_hashes_and_write_versions(
                &account_data,
                Vec::<&Hash>::new(),
                Vec::<StoredMetaWriteVersion>::new(),
            );

        let result = tiered_storage.write_accounts(&storable_accounts, 0);

        match (&result, &expected_result) {
            (
                Err(TieredStorageError::AttemptToUpdateReadOnly(_)),
                Err(TieredStorageError::AttemptToUpdateReadOnly(_)),
            ) => {}
            (Err(TieredStorageError::Unsupported()), Err(TieredStorageError::Unsupported())) => {}
            // we don't expect error type mis-match or other error types here
            _ => {
                panic!("actual: {result:?}, expected: {expected_result:?}");
            }
        };

        assert!(tiered_storage.is_read_only());
        assert_eq!(
            tiered_storage.file_size().unwrap() as usize,
            std::mem::size_of::<TieredStorageFooter>()
                + std::mem::size_of::<TieredStorageMagicNumber>()
        );
    }

    #[test]
    fn test_new_meta_file_only() {
        // Generate a new temp path that is guaranteed to NOT already have a file.
        let temp_dir = tempdir().unwrap();
        let tiered_storage_path = temp_dir.path().join("test_new_meta_file_only");

        {
            let tiered_storage = ManuallyDrop::new(TieredStorage::new_writable(
                &tiered_storage_path,
                HOT_FORMAT.clone(),
            ));

            assert!(!tiered_storage.is_read_only());
            assert_eq!(tiered_storage.path(), tiered_storage_path);
            assert_eq!(tiered_storage.file_size().unwrap(), 0);

            // Expect the result to be TieredStorageError::Unsupported as the feature
            // is not yet fully supported, but we can still check its partial results
            // in the test.
            write_zero_accounts(&tiered_storage, Err(TieredStorageError::Unsupported()));
        }

        let tiered_storage_readonly = TieredStorage::new_readonly(&tiered_storage_path).unwrap();
        let footer = tiered_storage_readonly.footer().unwrap();
        assert!(tiered_storage_readonly.is_read_only());
        assert_eq!(tiered_storage_readonly.reader().unwrap().num_accounts(), 0);
        assert_eq!(footer.account_meta_format, HOT_FORMAT.account_meta_format);
        assert_eq!(footer.owners_block_format, HOT_FORMAT.owners_block_format);
        assert_eq!(footer.account_index_format, HOT_FORMAT.account_index_format);
        assert_eq!(footer.account_block_format, HOT_FORMAT.account_block_format);
        assert_eq!(
            tiered_storage_readonly.file_size().unwrap() as usize,
            std::mem::size_of::<TieredStorageFooter>()
                + std::mem::size_of::<TieredStorageMagicNumber>()
        );
    }

    #[test]
    fn test_write_accounts_twice() {
        // Generate a new temp path that is guaranteed to NOT already have a file.
        let temp_dir = tempdir().unwrap();
        let tiered_storage_path = temp_dir.path().join("test_write_accounts_twice");

        let tiered_storage = TieredStorage::new_writable(&tiered_storage_path, HOT_FORMAT.clone());
        // Expect the result to be TieredStorageError::Unsupported as the feature
        // is not yet fully supported, but we can still check its partial results
        // in the test.
        write_zero_accounts(&tiered_storage, Err(TieredStorageError::Unsupported()));
        // Expect AttemptToUpdateReadOnly error as write_accounts can only
        // be invoked once.
        write_zero_accounts(
            &tiered_storage,
            Err(TieredStorageError::AttemptToUpdateReadOnly(
                tiered_storage_path,
            )),
        );
    }

    #[test]
    fn test_remove_on_drop() {
        // Generate a new temp path that is guaranteed to NOT already have a file.
        let temp_dir = tempdir().unwrap();
        let tiered_storage_path = temp_dir.path().join("test_remove_on_drop");
        {
            let tiered_storage =
                TieredStorage::new_writable(&tiered_storage_path, HOT_FORMAT.clone());
            write_zero_accounts(&tiered_storage, Err(TieredStorageError::Unsupported()));
        }
        // expect the file does not exists as it has been removed on drop
        assert!(!tiered_storage_path.try_exists().unwrap());

        {
            let tiered_storage = ManuallyDrop::new(TieredStorage::new_writable(
                &tiered_storage_path,
                HOT_FORMAT.clone(),
            ));
            write_zero_accounts(&tiered_storage, Err(TieredStorageError::Unsupported()));
        }
        // expect the file exists as we have ManuallyDrop this time.
        assert!(tiered_storage_path.try_exists().unwrap());

        {
            // open again in read-only mode with ManuallyDrop.
            _ = ManuallyDrop::new(TieredStorage::new_readonly(&tiered_storage_path).unwrap());
        }
        // again expect the file exists as we have ManuallyDrop.
        assert!(tiered_storage_path.try_exists().unwrap());

        {
            // open again without ManuallyDrop in read-only mode
            _ = TieredStorage::new_readonly(&tiered_storage_path).unwrap();
        }
        // expect the file does not exist as the file has been removed on drop
        assert!(!tiered_storage_path.try_exists().unwrap());
    }

    fn create_test_account(seed: u64) -> (StoredMeta, AccountSharedData) {
        let data_byte = (seed % 256) as u8;
        let account = Account {
            lamports: seed,
            data: (0..seed as usize).map(|_| data_byte).collect(),
            owner: Pubkey::new_unique(),
            executable: seed % 2 > 0,
            rent_epoch: if seed % 3 > 0 { seed as u64 } else { 0 },
        };

        let stored_meta = StoredMeta {
            write_version_obsolete: seed,
            pubkey: Pubkey::new_unique(),
            data_len: seed,
        };
        (stored_meta, AccountSharedData::from(account))
    }

    fn write_accounts_test_helper(
        path_suffix: &str,
        account_data_sizes: &[u64],
        format: TieredStorageFormat,
    ) {
        let accounts: Vec<(StoredMeta, AccountSharedData)> = account_data_sizes
            .iter()
            .map(|size| create_test_account(*size))
            .collect();

        let account_refs: Vec<(&Pubkey, &AccountSharedData)> = accounts
            .iter()
            .map(|account| (&account.0.pubkey, &account.1))
            .collect();

        // Slot information is not used here
        let account_data = (Slot::MAX, &account_refs[..]);
        let hashes: Vec<_> = (0..account_data_sizes.len())
            .map(|_| Hash::new_unique())
            .collect();
        let write_versions: Vec<_> = accounts
            .iter()
            .map(|account| account.0.write_version_obsolete)
            .collect();

        let storable_accounts =
            StorableAccountsWithHashesAndWriteVersions::new_with_hashes_and_write_versions(
                &account_data,
                hashes.iter().collect(),
                write_versions,
            );

        let temp_dir = tempdir().unwrap();
        let tiered_storage_path = temp_dir.path().join(path_suffix);
        let tiered_storage = TieredStorage::new_writable(&tiered_storage_path, format.clone());
        _ = tiered_storage.write_accounts(&storable_accounts, 0);

        verify_hot_storage(&tiered_storage, &storable_accounts, format);
    }

    fn verify_hot_storage<
        'a,
        'b,
        T: ReadableAccount + Sync,
        U: StorableAccounts<'a, T>,
        V: Borrow<Hash>,
    >(
        tiered_storage: &TieredStorage,
        accounts: &StorableAccountsWithHashesAndWriteVersions<'a, 'b, T, U, V>,
        format: TieredStorageFormat,
    ) {
        let reader = tiered_storage.reader().unwrap();
        assert_eq!(reader.num_accounts(), accounts.len());

        let mut account_block_size = 0;
        let num_accounts = accounts.accounts.len();
        for i in 0..num_accounts {
            let (account, _, hash, write_version) = accounts.get(i);
            // the size with padding
            account_block_size += ((account.unwrap().data().len() + 7) / 8) * 8;

            // optional fields size
            if *hash != Hash::default() {
                account_block_size += std::mem::size_of::<Hash>();
            }
            if write_version != u64::MAX {
                account_block_size += std::mem::size_of::<StoredMetaWriteVersion>();
            }
            if account.unwrap().rent_epoch() != u64::MAX {
                account_block_size += std::mem::size_of::<u64>();
            }
        }
        account_block_size += num_accounts * std::mem::size_of::<HotAccountMeta>();

        let footer = reader.footer();
        let expected_footer = TieredStorageFooter {
            account_meta_format: format.account_meta_format,
            owners_block_format: format.owners_block_format,
            account_index_format: format.account_index_format,
            account_block_format: format.account_block_format,
            account_entry_count: accounts.len() as u32,
            account_index_offset: account_block_size as u64,
            owners_offset: (account_block_size
                + format.account_index_format.entry_size() * num_accounts)
                as u64,
            hash: footer.hash.clone(),
            ..TieredStorageFooter::default()
        };

        // TODO(yhchiang): verify account meta and data

        assert_eq!(*footer, expected_footer);
    }

    #[test]
    fn test_write_accounts_small_accounts() {
        write_accounts_test_helper(
            "test_write_accounts_small_accounts",
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            HOT_FORMAT.clone(),
        );
    }
}
