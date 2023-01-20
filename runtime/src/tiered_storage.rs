//! docs/src/proposals/append-vec-storage.md

pub mod cold;
pub mod data_block;
pub mod file;
pub mod footer;
pub mod hot;
pub mod meta_entries;
pub mod reader;
pub mod writer;

use {
    crate::{
        account_storage::meta::{
            StorableAccountsWithHashesAndWriteVersions, StoredAccountInfo, StoredAccountMeta,
        },
        append_vec::{AppendVec, MatchAccountOwnerError},
        storable_accounts::StorableAccounts,
    },
    data_block::AccountDataBlockWriter,
    log::log_enabled,
    once_cell::sync::OnceCell,
    reader::TieredStorageReader,
    solana_sdk::{account::ReadableAccount, hash::Hash, pubkey::Pubkey},
    std::{
        borrow::Borrow,
        fs::{remove_file, OpenOptions},
        path::{Path, PathBuf},
    },
    writer::TieredStorageWriter,
};

pub const ACCOUNT_DATA_BLOCK_SIZE: usize = 4096;
pub const ACCOUNTS_DATA_STORAGE_FORMAT_VERSION: u64 = 1;

lazy_static! {
    pub static ref HASH_DEFAULT: Hash = Hash::default();
}

#[derive(Debug)]
pub struct TieredStorage {
    reader: OnceCell<TieredStorageReader>,
    path: PathBuf,
    remove_on_drop: bool,
}

impl Drop for TieredStorage {
    fn drop(&mut self) {
        if self.remove_on_drop {
            if let Err(_e) = remove_file(&self.path) {
                // promote this to panic soon.
                // disabled due to many false positive warnings while running tests.
                // blocked by rpc's upgrade to jsonrpc v17
                //error!("AppendVec failed to remove {:?}: {:?}", &self.path, e);
                inc_new_counter_info!("append_vec_drop_fail", 1);
            }
        }
    }
}

impl TieredStorage {
    pub fn new(file_path: &Path, create: bool) -> Self {
        if create {
            let _ignored = remove_file(file_path);
            Self {
                reader: OnceCell::<TieredStorageReader>::new(),
                path: file_path.to_path_buf(),
                remove_on_drop: true,
            }
        } else {
            let (accounts_file, _) = Self::new_from_file(file_path).unwrap();
            return accounts_file;
        }
    }

    pub fn new_from_file<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<(Self, usize)> {
        let reader = TieredStorageReader::new_from_path(path.as_ref())?;
        let count = reader.num_accounts();
        let reader_cell = OnceCell::<TieredStorageReader>::new();
        reader_cell.set(reader).unwrap();
        Ok((
            Self {
                reader: reader_cell,
                path: path.as_ref().to_path_buf(),
                remove_on_drop: true,
            },
            count,
        ))
    }

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

    // Returns the Vec of offsets corresponding to the input accounts to later
    // construct AccountInfo
    pub fn append_accounts<
        'a,
        'b,
        T: ReadableAccount + Sync,
        U: StorableAccounts<'a, T>,
        V: Borrow<Hash>,
    >(
        &self,
        accounts: &StorableAccountsWithHashesAndWriteVersions<'a, 'b, T, U, V>,
        skip: usize,
    ) -> Option<Vec<StoredAccountInfo>> {
        log::info!("TieredStorage::append_accounts(): file {:?}", self.path);
        if self.is_read_only() {
            log::error!("TieredStorage::append_accounts(): attempt to append accounts to read only file {:?}", self.path);
            return None;
        }

        let result: Option<Vec<StoredAccountInfo>>;
        {
            let writer = TieredStorageWriter::new(&self.path);
            result = writer.append_accounts(accounts, skip);
        }

        if self
            .reader
            .set(TieredStorageReader::new_from_path(&self.path).unwrap())
            .is_err()
        {
            panic!(
                "TieredStorage::append_accounts(): unable to create reader for file {:?}",
                self.path
            );
        }
        log::info!(
            "TieredStorage::append_accounts(): successfully appended {} accounts to file {:?}",
            accounts.len() - skip,
            self.path
        );
        result
    }

    pub fn file_size(&self) -> std::io::Result<u64> {
        let file = OpenOptions::new()
            .read(true)
            .create(false)
            .open(self.path.to_path_buf())?;
        Ok(file.metadata()?.len())
    }

    pub fn is_read_only(&self) -> bool {
        self.reader.get().is_some()
    }

    pub fn write_from_append_vec(&self, append_vec: &AppendVec) -> std::io::Result<()> {
        let writer = TieredStorageWriter::new(&self.path);
        let result = writer.write_from_append_vec(&append_vec);
        if result.is_ok() {
            if self
                .reader
                .set(TieredStorageReader::new_from_path(&self.path).unwrap())
                .is_ok()
            {
                return result;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "TieredStorageError::Reader failure",
                ));
            }
        }

        result
    }

    ///////////////////////////////////////////////////////////////////////////////

    pub fn set_no_remove_on_drop(&mut self) {
        self.remove_on_drop = false;
    }

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

    pub fn flush(&self) -> std::io::Result<()> {
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
}

#[cfg(test)]
pub mod tests {
    use {
        crate::{
            account_storage::meta::{StorableAccountsWithHashesAndWriteVersions, StoredMeta},
            append_vec::{
                test_utils::{create_test_account_from_len, get_append_vec_path, TempFile},
                AppendVec,
            },
            tiered_storage::{
                cold::ColdAccountMeta,
                footer::{
                    AccountDataBlockFormat, AccountIndexFormat, AccountMetaFormat,
                    OwnersBlockFormat, TieredStorageFooter, FOOTER_SIZE,
                },
                meta_entries::ACCOUNT_META_ENTRY_SIZE_BYTES,
                reader::TieredStorageReader,
                TieredStorage, ACCOUNTS_DATA_STORAGE_FORMAT_VERSION, ACCOUNT_DATA_BLOCK_SIZE,
            },
        },
        once_cell::sync::OnceCell,
        solana_sdk::{account::AccountSharedData, clock::Slot, hash::Hash, pubkey::Pubkey},
        std::{collections::HashMap, mem, path::Path},
    };

    impl TieredStorage {
        fn new_for_test(file_path: &Path) -> Self {
            Self {
                reader: OnceCell::<TieredStorageReader>::new(),
                path: file_path.to_path_buf(),
                remove_on_drop: false,
            }
        }
        fn footer(&self) -> Option<&TieredStorageFooter> {
            if let Some(reader) = self.reader.get() {
                return Some(reader.footer());
            }
            None
        }
        fn metas(&self) -> Option<&Vec<ColdAccountMeta>> {
            if let Some(reader) = self.reader.get() {
                return Some(reader.metas());
            }
            None
        }
    }

    impl TieredStorageReader {
        fn footer(&self) -> &TieredStorageFooter {
            match self {
                Self::Cold(cs) => &cs.footer,
            }
        }
        fn metas(&self) -> &Vec<ColdAccountMeta> {
            match self {
                Self::Cold(cs) => &cs.metas,
            }
        }
    }

    /*
    #[test]
    fn test_account_metas_block() {
        let path = get_append_vec_path("test_account_metas_block");

        const ENTRY_COUNT: u64 = 128;
        const TEST_LAMPORT_BASE: u64 = 48372;
        const BLOCK_OFFSET_BASE: u64 = 3423;
        const DATA_LENGTH: u16 = 976;
        const TEST_RENT_EPOCH: Epoch = 327;
        const TEST_WRITE_VERSION: StoredMetaWriteVersion = 543432;
        let mut expected_metas: Vec<ColdAccountMeta> = vec![];

        {
            let ads = TieredStorageWriter::new(&path.path);
            let mut footer = TieredStorageFooter::new();
            let mut cursor = 0;
            let meta_per_block = (ACCOUNT_DATA_BLOCK_SIZE as u16) / DATA_LENGTH;
            for i in 0..ENTRY_COUNT {
                expected_metas.push(
                    ColdAccountMeta::new()
                        .with_lamports(i * TEST_LAMPORT_BASE)
                        .with_block_offset(i * BLOCK_OFFSET_BASE)
                        .with_owner_local_id(i as u32)
                        .with_uncompressed_data_size(DATA_LENGTH)
                        .with_intra_block_offset(((i as u16) % meta_per_block) * DATA_LENGTH)
                        .with_flags(
                            AccountMetaFlags::new()
                                .with_bit(AccountMetaFlags::EXECUTABLE, i % 2 == 0)
                                .to_value(),
                        )
                        .with_optional_fields(&AccountMetaOptionalFields {
                            rent_epoch: if i % 2 == 1 {
                                Some(TEST_RENT_EPOCH)
                            } else {
                                None
                            },
                            account_hash: if i % 2 == 0 {
                                Some(Hash::new_unique())
                            } else {
                                None
                            },
                            write_version_obsolete: if i % 2 == 1 {
                                Some(TEST_WRITE_VERSION)
                            } else {
                                None
                            },
                        }),
                );
            }
            ads.write_account_metas_block(&mut cursor, &mut footer, &expected_metas)
                .unwrap();
        }

        let ads = TieredStorage::new_for_test(&path.path, false);
        let metas: Vec<ColdAccountMeta> =
            ads.read_account_metas_block(0, ENTRY_COUNT as u32).unwrap();
        assert_eq!(expected_metas, metas);
        for i in 0..ENTRY_COUNT as usize {
            assert_eq!(
                metas[i].flags_get(AccountMetaFlags::HAS_RENT_EPOCH),
                i % 2 == 1
            );
            assert_eq!(
                metas[i].flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH),
                i % 2 == 0
            );
            assert_eq!(
                metas[i].flags_get(AccountMetaFlags::HAS_WRITE_VERSION),
                i % 2 == 1
            );
        }
    }*/

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

    fn ads_writer_test_help(path_prefix: &str, account_data_sizes: &[usize]) {
        write_from_append_vec_test_helper(
            &(path_prefix.to_owned() + "_from_append_vec"),
            account_data_sizes,
        );
        append_accounts_test_helper(
            &(path_prefix.to_owned() + "_append_accounts"),
            account_data_sizes,
        );
    }

    fn append_accounts_test_helper(path_prefix: &str, account_data_sizes: &[usize]) {
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
            let ads = TieredStorage::new_for_test(&ads_path.path);
            ads.append_accounts(&storable_accounts, 0);
        }

        verify_account_data_storage(account_count, &test_accounts, &ads_path, &hashes_map);
    }

    fn write_from_append_vec_test_helper(path_prefix: &str, account_data_sizes: &[usize]) {
        let account_count = account_data_sizes.len();
        let (test_accounts, av) =
            create_test_append_vec(&(path_prefix.to_owned() + "_av"), account_data_sizes);

        let ads_path = get_append_vec_path(&(path_prefix.to_owned() + "_ads"));
        {
            let ads = TieredStorage::new_for_test(&ads_path.path);
            ads.write_from_append_vec(&av).unwrap();
        }

        verify_account_data_storage(account_count, &test_accounts, &ads_path, &HashMap::new());
    }

    /*
    fn verify_account_data_storage2(
        account_count: usize,
        test_accounts: &HashMap<Pubkey, (StoredMeta, AccountSharedData)>,
        ads_path: &TempFile,
        hashes_map: &HashMap<Pubkey, &Hash>,
    ) {
        let ads = TieredStorage::new(&ads_path.path, false);
        let footer = ads.footer().unwrap();

        let expected_footer = TieredStorageFooter {
            account_meta_count: account_count as u32,
            account_meta_entry_size: ACCOUNT_META_ENTRY_SIZE_BYTES,
            account_data_block_size: ACCOUNT_DATA_BLOCK_SIZE as u64,
            owner_count: account_count as u32,
            owner_entry_size: mem::size_of::<Pubkey>() as u32,
            // This number should be the total compressed account data size.
            account_metas_offset: footer.account_metas_offset,
            account_pubkeys_offset: footer.account_pubkeys_offset,
            owners_offset: footer.account_pubkeys_offset
                + (account_count * mem::size_of::<Pubkey>()) as u64,
            // TODO(yhchiang): not yet implemented
            data_block_format: AccountDataBlockFormat::Lz4,
            // TODO(yhchiang): not yet implemented
            hash: footer.hash,
            // TODO(yhchiang): fix this
            min_account_address: Hash::default(),
            max_account_address: Hash::default(),
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
            let expected_hash = &hashes_map[account.pubkey()];
            verify_account(&account, expected_account);
            assert_eq!(account.hash(), *expected_hash);
        }
        assert_eq!(&count_from_ads, &account_count);
    }
    */

    fn verify_account_data_storage(
        account_count: usize,
        test_accounts: &HashMap<Pubkey, (StoredMeta, AccountSharedData)>,
        ads_path: &TempFile,
        hashes_map: &HashMap<Pubkey, &Hash>,
    ) {
        let ads = TieredStorage::new(&ads_path.path, false);
        let footer = ads.footer().unwrap();

        let expected_footer = TieredStorageFooter {
            account_meta_format: AccountMetaFormat::Cold,
            owners_block_format: OwnersBlockFormat::LocalIndex,
            account_index_format: AccountIndexFormat::Linear,
            data_block_format: AccountDataBlockFormat::Lz4,
            account_meta_count: account_count as u32,
            account_meta_entry_size: ACCOUNT_META_ENTRY_SIZE_BYTES,
            account_data_block_size: ACCOUNT_DATA_BLOCK_SIZE as u64,
            owner_count: account_count as u32,
            owner_entry_size: mem::size_of::<Pubkey>() as u32,
            // This number should be the total compressed account data size.
            account_metas_offset: footer.account_metas_offset,
            account_pubkeys_offset: footer.account_pubkeys_offset,
            owners_offset: footer.account_pubkeys_offset
                + (account_count * mem::size_of::<Pubkey>()) as u64,
            // TODO(yhchiang): not yet implemented
            // TODO(yhchiang): not yet implemented
            hash: footer.hash,
            // TODO(yhchiang): fix this
            min_account_address: Hash::default(),
            max_account_address: Hash::default(),
            format_version: ACCOUNTS_DATA_STORAGE_FORMAT_VERSION,
            footer_size: FOOTER_SIZE as u64,
        };
        assert_eq!(*footer, expected_footer);

        let metas = ads.metas().unwrap();
        assert_eq!(metas.len(), account_count);

        let mut index = 0;
        let mut count_from_ads = 0;
        while let Some((account, next)) = ads.get_account(index) {
            index = next;
            count_from_ads += 1;
            let expected_account = &test_accounts[account.pubkey()];
            assert_eq!(account.clone_account(), expected_account.1);

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
        ads_writer_test_help("test_write_from_append_vec_one_small", &[255]);
    }

    #[test]
    fn test_write_from_append_vec_one_big() {
        ads_writer_test_help("test_write_from_append_vec_one_big", &[25500]);
    }

    #[test]
    fn test_write_from_append_vec_one_10_mb() {
        ads_writer_test_help("test_write_from_append_vec_one_10_mb", &[10 * 1024 * 1024]);
    }

    #[test]
    fn test_write_from_append_vec_multiple_blobs() {
        ads_writer_test_help(
            "test_write_from_append_vec_multiple_blobs",
            &[5000, 6000, 7000, 8000, 5500, 10241023, 9999],
        );
    }

    #[test]
    fn test_write_from_append_vec_one_data_block() {
        ads_writer_test_help(
            "test_write_from_append_vec_one_data_block",
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        );
    }

    #[test]
    fn test_write_from_append_vec_mixed_block() {
        ads_writer_test_help(
            "test_write_from_append_vec_mixed_block",
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000, 2000, 3000, 4000, 9, 8, 7, 6, 5, 4, 3, 2, 1,
            ],
        );
    }
}
