#![allow(dead_code)]

pub mod byte_block;
pub mod cold;
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
        account_storage::meta::{
            StorableAccountsWithHashesAndWriteVersions, StoredAccountInfo, StoredAccountMeta,
        },
        append_vec::MatchAccountOwnerError,
        storable_accounts::StorableAccounts,
    },
    error::TieredStorageError,
    footer::{AccountBlockFormat, AccountMetaFormat, OwnersBlockFormat},
    index::AccountIndexFormat,
    once_cell::sync::OnceCell,
    readable::TieredStorageReader,
    solana_sdk::{account::ReadableAccount, hash::Hash, pubkey::Pubkey},
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

// BEGIN OF FUTURE CODE
pub const ACCOUNT_DATA_BLOCK_SIZE: usize = 4096;
pub const ACCOUNTS_DATA_STORAGE_FORMAT_VERSION: u64 = 1;

lazy_static! {
    pub static ref HASH_DEFAULT: Hash = Hash::default();
}
// END OF FUTURE CODE

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
    // TODO(yhchiang): this was new()
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

    // TODO(yhchiang): this was new_from_path -> (Self, usize)
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

    // BEGIN OF FUTURE CODE
    pub fn account_matches_owners(
        &self,
        multiplied_index: usize,
        owners: &[&Pubkey],
    ) -> Result<usize, MatchAccountOwnerError> {
        if let Some(reader) = self.reader.get() {
            return reader.account_matches_owners(multiplied_index, owners);
        }

        Err(MatchAccountOwnerError::UnableToLoad)
    }

    pub fn get_account<'a>(
        &'a self,
        multiplied_index: usize,
    ) -> Option<(StoredAccountMeta<'a>, usize)> {
        if multiplied_index % 1024 == 0 {
            log::info!(
                "TieredStorage::get_account(): fetch {} account at file {:?}",
                multiplied_index,
                self.path
            );
        }
        if let Some(reader) = self.reader.get() {
            return reader.get_account(multiplied_index);
        }
        None
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn accounts(&self, mut multiplied_index: usize) -> Vec<StoredAccountMeta> {
        log::info!(
            "TieredStorage::accounts(): fetch all accounts after {} at file {:?}",
            multiplied_index,
            self.path
        );
        let mut accounts = vec![];
        while let Some((account, next)) = self.get_account(multiplied_index) {
            accounts.push(account);
            multiplied_index = next;
        }
        accounts
    }
    // END OF FUTURE CODE

    /// Returns the underlying reader of the TieredStorage.  None will be
    /// returned if it's is_read_only() returns false.
    pub fn reader(&self) -> Option<&TieredStorageReader> {
        self.reader.get()
    }

    /// Returns true if the TieredStorage instance is read-only.
    pub fn is_read_only(&self) -> bool {
        self.reader.get().is_some()
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
        log::info!("TieredStorage::write_accounts(): file {:?}", self.path);
        if self.is_read_only() {
            log::error!("TieredStorage::write_accounts(): attempt to append accounts to read only file {:?}", self.path);

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

        log::info!(
            "TieredStorage::write_accounts(): successfully appended {} accounts to file {:?}",
            accounts.len() - skip,
            self.path
        );

        result
    }

    /// Returns the size of the underlying accounts file.
    pub fn file_size(&self) -> TieredStorageResult<u64> {
        let file = OpenOptions::new().read(true).open(&self.path);

        Ok(file
            .and_then(|file| file.metadata())
            .map(|metadata| metadata.len())
            .unwrap_or(0))
    }

    // BEGIN OF FUTURE CODE
    /*
    pub fn write_from_append_vec(&self, append_vec: &AppendVec) -> TieredStorageResult<()> {
        let writer = TieredStorageWriter::new(&self.path, self.format.unwrap());
        writer.write_from_append_vec(&append_vec)?;

        self.reader
            .set(TieredStorageReader::new_from_path(&self.path)?)
            .map_err(|_| TieredStorageError::ReaderInitializationFailure())
    }*/

    ///////////////////////////////////////////////////////////////////////////////

    pub fn remaining_bytes(&self) -> u64 {
        if self.is_read_only() {
            return 0;
        }
        std::u64::MAX
    }

    pub fn len(&self) -> usize {
        self.file_size().unwrap_or(0).try_into().unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    ///////////////////////////////////////////////////////////////////////////////
    // unimplemented

    pub fn flush(&self) -> TieredStorageResult<()> {
        Ok(())
    }

    pub fn reset(&self) {}

    pub fn capacity(&self) -> u64 {
        if self.is_read_only() {
            return self.len().try_into().unwrap();
        }
        self.len().try_into().unwrap()
    }

    pub fn is_ancient(&self) -> bool {
        false
    }
    // END OF FUTURE CODE
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            account_storage::meta::{
                StorableAccountsWithHashesAndWriteVersions, StoredMeta, StoredMetaWriteVersion,
            },
            append_vec::{
                test_utils::{create_test_account_from_len, get_append_vec_path, TempFile},
                AppendVec,
            },
        },
        footer::{TieredStorageFooter, TieredStorageMagicNumber, FOOTER_SIZE},
        // cold::COLD_FORMAT,
        hot::HOT_FORMAT,
        index::AccountIndexFormat,
        once_cell::sync::OnceCell,
        readable::TieredStorageReader,
        solana_sdk::{
            account::{AccountSharedData, ReadableAccount},
            clock::Slot,
            hash::Hash,
            pubkey::Pubkey,
        },
        std::{collections::HashMap, mem, mem::ManuallyDrop, path::Path},
        tempfile::tempdir,
        TieredStorage,
        TieredStorageFormat,
        ACCOUNTS_DATA_STORAGE_FORMAT_VERSION,
        ACCOUNT_DATA_BLOCK_SIZE,
    };

    impl TieredStorage {
        // BEGIN OF FUTURE CODE
        fn new_for_test(file_path: &Path, format: TieredStorageFormat) -> ManuallyDrop<Self> {
            ManuallyDrop::new(Self {
                reader: OnceCell::<TieredStorageReader>::new(),
                format: Some(format),
                path: file_path.to_path_buf(),
            })
        }
        // END OF FUTURE CODE

        fn footer(&self) -> Option<&TieredStorageFooter> {
            self.reader.get().map(|r| r.footer())
        }
    }

    // END OF FUTURE CODE

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
            (Ok(_), Ok(_)) => {}
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

            write_zero_accounts(&tiered_storage, Ok(vec![]));
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
        write_zero_accounts(&tiered_storage, Ok(vec![]));
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
            write_zero_accounts(&tiered_storage, Ok(vec![]));
        }
        // expect the file does not exists as it has been removed on drop
        assert!(!tiered_storage_path.try_exists().unwrap());

        {
            let tiered_storage = ManuallyDrop::new(TieredStorage::new_writable(
                &tiered_storage_path,
                HOT_FORMAT.clone(),
            ));
            write_zero_accounts(&tiered_storage, Ok(vec![]));
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

    // BEGIN OF FUTURE CODE
    fn create_test_append_vec(
        path: &str,
        data_sizes: &[usize],
    ) -> (HashMap<Pubkey, (StoredMeta, AccountSharedData)>, AppendVec) {
        let av_path = get_append_vec_path(path);
        let av = AppendVec::new(&av_path.path, true, 100 * 1024 * 1024);
        let mut test_accounts: HashMap<Pubkey, (StoredMeta, AccountSharedData)> = HashMap::new();

        for size in data_sizes {
            let account = create_test_account_from_len(*size);
            let index = av.append_account_test(&account).unwrap();
            assert_eq!(av.get_account_test(index).unwrap(), account);
            test_accounts.insert(account.0.pubkey, account);
        }

        (test_accounts, av)
    }

    fn ads_writer_test_help(
        path_prefix: &str,
        account_data_sizes: &[usize],
        format: TieredStorageFormat,
    ) {
        /*
        write_from_append_vec_test_helper(
            &(path_prefix.to_owned() + "_from_append_vec"),
            account_data_sizes,
        );
        */
        write_accounts_test_helper(
            &(path_prefix.to_owned() + "_write_accounts"),
            account_data_sizes,
            format,
        );
    }

    fn write_accounts_test_helper(
        path_prefix: &str,
        account_data_sizes: &[usize],
        format: TieredStorageFormat,
    ) {
        let account_count = account_data_sizes.len();
        let (test_accounts, _av) =
            create_test_append_vec(&(path_prefix.to_owned() + "_av"), account_data_sizes);

        let slot_ignored = Slot::MAX;
        let accounts: Vec<(Pubkey, AccountSharedData)> = test_accounts
            .clone()
            .into_iter()
            .map(|(pubkey, acc)| (pubkey, acc.1))
            .collect();
        let mut accounts_ref: Vec<(&Pubkey, &AccountSharedData)> = Vec::new();

        for (x, y) in &accounts {
            accounts_ref.push((&x, &y));
        }

        let slice = &accounts_ref[..];
        let account_data = (slot_ignored, slice);
        let mut write_versions = Vec::new();

        for (_pubkey, acc) in &test_accounts {
            write_versions.push(acc.0.write_version_obsolete);
        }

        let mut hashes = Vec::new();
        let mut hashes_ref = Vec::new();
        let mut hashes_map = HashMap::new();

        for _ in 0..write_versions.len() {
            hashes.push(Hash::new_unique());
        }
        for i in 0..write_versions.len() {
            hashes_ref.push(&hashes[i]);
        }
        for i in 0..write_versions.len() {
            hashes_map.insert(accounts[i].0, &hashes[i]);
        }

        let storable_accounts =
            StorableAccountsWithHashesAndWriteVersions::new_with_hashes_and_write_versions(
                &account_data,
                hashes_ref,
                write_versions,
            );

        let ads_path = get_append_vec_path(&(path_prefix.to_owned() + "_ads"));
        {
            let ads = TieredStorage::new_for_test(&ads_path.path, format.clone());
            ads.write_accounts(&storable_accounts, 0).unwrap();
        }

        verify_account_data_storage(
            account_count,
            &test_accounts,
            &ads_path,
            &hashes_map,
            format,
        );
    }

    /*
    fn write_from_append_vec_test_helper(
        path_prefix: &str,
        account_data_sizes: &[usize],
        format: TieredStorageFormat,
    ) {
        let account_count = account_data_sizes.len();
        let (test_accounts, av) =
            create_test_append_vec(&(path_prefix.to_owned() + "_av"), account_data_sizes);

        let ads_path = get_append_vec_path(&(path_prefix.to_owned() + "_ads"));
        {
            let ads = TieredStorage::new_for_test(&ads_path.path, format);
            ads.write_from_append_vec(&av).unwrap();
        }

        verify_account_data_storage(
            account_count,
            &test_accounts,
            &ads_path,
            &HashMap::new(),
            format,
        );
    }
    */

    fn verify_account_data_storage(
        account_count: usize,
        test_accounts: &HashMap<Pubkey, (StoredMeta, AccountSharedData)>,
        ads_path: &TempFile,
        hashes_map: &HashMap<Pubkey, &Hash>,
        format: TieredStorageFormat,
    ) {
        let ads = TieredStorage::new_readonly(&ads_path.path).unwrap();
        let footer = ads.footer().unwrap();
        let indexer = AccountIndexFormat::AddressAndOffset;

        let expected_footer = TieredStorageFooter {
            account_meta_format: format.account_meta_format.clone(),
            owners_block_format: format.owners_block_format.clone(),
            account_index_format: format.account_index_format.clone(),
            account_block_format: format.account_block_format.clone(),
            account_entry_count: account_count as u32,
            account_meta_entry_size: format.meta_entry_size as u32,
            account_block_size: ACCOUNT_DATA_BLOCK_SIZE as u64,
            owner_count: account_count as u32,
            owner_entry_size: mem::size_of::<Pubkey>() as u32,
            // This number should be the total compressed account data size.
            account_index_offset: footer.account_index_offset,
            owners_offset: footer.account_index_offset
                + (account_count * indexer.entry_size()) as u64,
            // TODO(yhchiang): reach out Brooks on how to obtain the new hash
            hash: footer.hash,
            // TODO(yhchiang): fix this
            min_account_address: Pubkey::default(),
            max_account_address: Pubkey::default(),
            format_version: ACCOUNTS_DATA_STORAGE_FORMAT_VERSION,
            footer_size: FOOTER_SIZE as u64,
        };
        assert_eq!(*footer, expected_footer);

        let mut index = 0;
        let mut count_from_ads = 0;
        while let Some((account, next)) = ads.get_account(index) {
            index = next;
            count_from_ads += 1;
            let expected_account = &test_accounts[account.pubkey()];
            assert_eq!(account.to_account_shared_data(), expected_account.1);

            if hashes_map.len() > 0 {
                let expected_hash = &hashes_map[account.pubkey()];
                assert_eq!(account.hash(), *expected_hash);
            }

            let stored_meta_from_storage = StoredMeta {
                write_version_obsolete: account.write_version(),
                pubkey: *account.pubkey(),
                data_len: account.data_len(),
            };
            assert_eq!(stored_meta_from_storage, expected_account.0);
        }
        assert_eq!(&count_from_ads, &account_count);
    }

    #[test]
    fn test_write_from_append_vec_one_small() {
        ads_writer_test_help(
            "test_write_from_append_vec_one_small_hot",
            &[255],
            HOT_FORMAT.clone(),
        );
        ads_writer_test_help(
            "test_write_from_append_vec_one_small_cold",
            &[255],
            HOT_FORMAT.clone(), //&COLD_FORMAT YHCHIANG
        );
    }

    #[test]
    fn test_write_from_append_vec_one_big() {
        ads_writer_test_help(
            "test_write_from_append_vec_one_big_hot",
            &[25500],
            HOT_FORMAT.clone(),
        );
        ads_writer_test_help(
            "test_write_from_append_vec_one_big_cold",
            &[25500],
            HOT_FORMAT.clone(), //&COLD_FORMAT YHCHIANG
        );
    }

    #[test]
    fn test_write_from_append_vec_one_10_mb() {
        ads_writer_test_help(
            "test_write_from_append_vec_one_10_mb_hot",
            &[10 * 1024 * 1024],
            HOT_FORMAT.clone(),
        );
        ads_writer_test_help(
            "test_write_from_append_vec_one_10_mb_cold",
            &[10 * 1024 * 1024],
            HOT_FORMAT.clone(), //&COLD_FORMAT YHCHIANG
        );
    }

    #[test]
    fn test_write_from_append_vec_multiple_blobs() {
        ads_writer_test_help(
            "test_write_from_append_vec_multiple_blobs_hot",
            &[5000, 6000, 7000, 8000, 5500, 10241023, 9999],
            HOT_FORMAT.clone(),
        );
        ads_writer_test_help(
            "test_write_from_append_vec_multiple_blobs_cold",
            &[5000, 6000, 7000, 8000, 5500, 10241023, 9999],
            HOT_FORMAT.clone(), //&COLD_FORMAT YHCHIANG
        );
    }

    #[test]
    fn test_write_from_append_vec_one_data_block() {
        ads_writer_test_help(
            "test_write_from_append_vec_one_data_block_hot",
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            HOT_FORMAT.clone(),
        );
        ads_writer_test_help(
            "test_write_from_append_vec_one_data_block_cold",
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            HOT_FORMAT.clone(), //&COLD_FORMAT YHCHIANG
        );
    }

    #[test]
    fn test_write_from_append_vec_mixed_block() {
        ads_writer_test_help(
            "test_write_from_append_vec_mixed_block_hot",
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000, 2000, 3000, 4000, 9, 8, 7, 6, 5, 4, 3, 2, 1,
            ],
            HOT_FORMAT.clone(),
        );
        ads_writer_test_help(
            "test_write_from_append_vec_mixed_block_cold",
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000, 2000, 3000, 4000, 9, 8, 7, 6, 5, 4, 3, 2, 1,
            ],
            HOT_FORMAT.clone(), //&COLD_FORMAT YHCHIANG
        );
    }
    // END OF FUTURE CODE
}
