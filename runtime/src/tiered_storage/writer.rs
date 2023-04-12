//! docs/src/proposals/append-vec-storage.md

use {
    crate::{
        account_storage::meta::{
            AccountMeta, StorableAccountsWithHashesAndWriteVersions, StoredAccountInfo,
            StoredAccountMeta, StoredMeta,
        },
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        append_vec::{AppendVec, AppendVecStoredAccountMeta},
        storable_accounts::StorableAccounts,
        tiered_storage::{
            byte_block::ByteBlockWriter,
            file::TieredStorageFile,
            footer::{AccountMetaFormat, TieredFileFormat, TieredStorageFooter},
            hot::HotAccountMeta,
            index::{AccountIndexWriterEntry, HotAccountIndexer},
            meta::{AccountMetaFlags, AccountMetaOptionalFields, TieredAccountMeta},
            TieredStorageResult,
        },
    },
    log::*,
    solana_sdk::{account::ReadableAccount, hash::Hash, pubkey::Pubkey},
    std::{borrow::Borrow, collections::HashMap, fs::remove_file, mem, path::Path},
};

pub const ACCOUNT_DATA_BLOCK_SIZE: usize = 4096;
pub const ACCOUNTS_DATA_STORAGE_FORMAT_VERSION: u64 = 1;

lazy_static! {
    pub static ref HASH_DEFAULT: Hash = Hash::default();
}

pub(crate) struct AccountOwnerTable {
    pub owners_vec: Vec<Pubkey>,
    pub owners_map: HashMap<Pubkey, u32>,
}

impl AccountOwnerTable {
    pub fn new() -> Self {
        Self {
            owners_vec: vec![],
            owners_map: HashMap::new(),
        }
    }
    pub fn check_and_add(&mut self, pubkey: &Pubkey) -> u32 {
        if let Some(index) = self.owners_map.get(pubkey) {
            return index.clone();
        }
        let index: u32 = self.owners_vec.len().try_into().unwrap();
        self.owners_vec.push(*pubkey);
        self.owners_map.insert(*pubkey, index);

        index
    }
}

#[derive(Debug)]
pub struct TieredStorageWriter {
    storage: TieredStorageFile,
    format: &'static TieredFileFormat,
}

impl TieredStorageWriter {
    /// Create a new accounts-state-file
    #[allow(dead_code)]
    pub fn new(file_path: &Path, format: &'static TieredFileFormat) -> Self {
        let _ignored = remove_file(file_path);
        Self {
            storage: TieredStorageFile::new_writable(file_path),
            format: format,
        }
    }

    fn append_accounts_impl<
        'a,
        'b,
        T: ReadableAccount + Sync,
        U: StorableAccounts<'a, T>,
        V: Borrow<Hash>,
        W: TieredAccountMeta,
    >(
        &self,
        accounts: &StorableAccountsWithHashesAndWriteVersions<'a, 'b, T, U, V>,
        mut footer: TieredStorageFooter,
        mut account_metas: Vec<W>,
        skip: usize,
    ) -> Option<Vec<StoredAccountInfo>> {
        let mut cursor = 0;
        let mut account_pubkeys: Vec<Pubkey> = vec![];
        let mut owners_table = AccountOwnerTable::new();
        let mut dummy_hash: Hash = Hash::new_unique();

        let mut data_block_writer = self.new_data_block_writer(&footer);
        footer.account_block_size = ACCOUNT_DATA_BLOCK_SIZE as u64;
        footer.account_meta_entry_size = std::mem::size_of::<W>() as u32;

        let mut buffered_account_metas = Vec::<W>::new();
        let mut buffered_account_pubkeys: Vec<Pubkey> = vec![];

        let len = accounts.accounts.len();
        let mut input_pubkey_map: HashMap<Pubkey, usize> = HashMap::with_capacity(len);
        let mut account_index_entries = Vec::<AccountIndexWriterEntry>::new();

        for i in skip..len {
            // TODO(yhchiang): here we don't need to convert it to
            // StoredAccountMeta::AppendVec
            let (account, pubkey, hash, write_version_obsolete) = accounts.get(i);
            input_pubkey_map.insert(*pubkey, i);
            let account_meta = account
                .map(|account| AccountMeta {
                    lamports: account.lamports(),
                    owner: *account.owner(),
                    rent_epoch: account.rent_epoch(),
                    executable: account.executable(),
                })
                .unwrap_or_default();

            let stored_meta = StoredMeta {
                pubkey: *pubkey,
                data_len: account
                    .map(|account| account.data().len())
                    .unwrap_or_default() as u64,
                write_version_obsolete,
            };

            let stored_account_meta = StoredAccountMeta::AppendVec(AppendVecStoredAccountMeta {
                meta: &stored_meta,
                account_meta: &account_meta,
                data: account.map(|account| account.data()).unwrap_or_default(),
                offset: 0,
                stored_size: 0,
                hash: hash,
            });

            data_block_writer = self
                .write_stored_account_meta(
                    &stored_account_meta,
                    &mut cursor,
                    &mut footer,
                    &mut account_metas,
                    &mut account_pubkeys,
                    &mut owners_table,
                    data_block_writer,
                    &mut buffered_account_metas,
                    &mut buffered_account_pubkeys,
                    &mut dummy_hash,
                    &mut account_index_entries,
                )
                .unwrap();
        }

        // Persist the last block if any
        if buffered_account_metas.len() > 0 {
            self.flush_account_block(
                &mut cursor,
                &mut footer,
                &mut account_metas,
                &mut account_pubkeys,
                &mut buffered_account_metas,
                &mut buffered_account_pubkeys,
                data_block_writer,
            )
            .ok()?;
        }

        assert_eq!(buffered_account_metas.len(), 0);
        assert_eq!(buffered_account_pubkeys.len(), 0);
        assert_eq!(footer.account_entry_count, account_metas.len() as u32);

        self.write_account_pubkeys_block(&mut cursor, &mut footer, &account_index_entries)
            .ok()?;

        self.write_owners_block(&mut cursor, &mut footer, &owners_table.owners_vec)
            .ok()?;

        footer.write_footer_block(&self.storage).ok()?;

        assert_eq!(account_metas.len(), account_pubkeys.len());
        assert_eq!(account_metas.len(), len - skip);

        let mut stored_accounts_info: Vec<StoredAccountInfo> = Vec::with_capacity(len);
        for _ in skip..len {
            stored_accounts_info.push(StoredAccountInfo { offset: 0, size: 0 });
        }
        for i in 0..account_metas.len() {
            let index = input_pubkey_map.get(&account_pubkeys[i]).unwrap();

            // of ALIGN_BOUNDARY_OFFSET, while cold storage actually talks about index
            // instead of offset.
            stored_accounts_info[*index].offset = i * ALIGN_BOUNDARY_OFFSET;
            stored_accounts_info[*index].size = W::stored_size(&footer, &account_metas, i);
        }
        match footer.account_meta_format {
            AccountMetaFormat::Hot => info!(
                "[Hot] append_accounts successfully completed. Footer: {:?}",
                footer
            ),
            /*
            AccountMetaFormat::Cold => info!(
                "[Cold] append_accounts successfully completed. Footer: {:?}",
                footer
            ),
            */
        }

        Some(stored_accounts_info)
    }

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
        let mut footer = TieredStorageFooter::default();
        // TODO(yhchiang): make it configerable
        footer.account_meta_format = self.format.account_meta_format.clone();
        footer.account_block_format = self.format.account_block_format.clone();
        footer.format_version = ACCOUNTS_DATA_STORAGE_FORMAT_VERSION;
        match footer.account_meta_format {
            AccountMetaFormat::Hot => {
                info!(
                    "[Hot] Appending {} accounts to hot storage.",
                    accounts.len() - skip
                );
                self.append_accounts_impl(accounts, footer, Vec::<HotAccountMeta>::new(), skip)
            } /*
              AccountMetaFormat::Cold => {
                  info!(
                      "[Cold] Appending {} accounts to cold storage.",
                      accounts.len() - skip
                  );
                  self.append_accounts_impl(accounts, footer, Vec::<ColdAccountMeta>::new(), skip)
              }*/
        }
    }

    fn new_data_block_writer(&self, footer: &TieredStorageFooter) -> ByteBlockWriter {
        return ByteBlockWriter::new(footer.account_block_format);
    }

    pub(crate) fn write_account_pubkeys_block(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        index_entries: &Vec<AccountIndexWriterEntry>,
    ) -> TieredStorageResult<()> {
        footer.account_index_offset = *cursor;
        match footer.account_meta_format {
            AccountMetaFormat::Hot => {
                *cursor += HotAccountIndexer::write_index_block(&self.storage, index_entries)?;
            } // AccountMetaFormat::Cold => unimplemented!(),
        }
        Ok(())
    }

    fn write_owners_block(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        pubkeys: &Vec<Pubkey>,
    ) -> TieredStorageResult<()> {
        footer.owners_offset = *cursor;
        footer.owner_count = pubkeys.len() as u32;
        footer.owner_entry_size = mem::size_of::<Pubkey>() as u32;

        self.write_pubkeys_block(cursor, pubkeys)
    }

    fn write_pubkeys_block(
        &self,
        cursor: &mut u64,
        pubkeys: &Vec<Pubkey>,
    ) -> TieredStorageResult<()> {
        for pubkey in pubkeys {
            *cursor += self.storage.write_type(pubkey)? as u64;
        }

        Ok(())
    }

    fn flush_account_block<T: TieredAccountMeta>(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        account_metas: &mut Vec<T>,
        account_pubkeys: &mut Vec<Pubkey>,
        input_metas: &mut Vec<T>,
        input_pubkeys: &mut Vec<Pubkey>,
        data_block_writer: ByteBlockWriter,
    ) -> TieredStorageResult<()> {
        // Persist the current block
        let encoded_data = data_block_writer.finish()?;
        self.storage.write_bytes(&encoded_data)?;

        assert_eq!(input_metas.len(), input_pubkeys.len());

        /*
        for input_meta in &mut input_metas.into_iter() {
            input_meta.set_block_offset(*cursor);
        }
        for input_meta in &mut input_metas.into_iter() {
            assert_eq!(input_meta.block_offset(), *cursor);
        }
        */

        footer.account_entry_count += input_metas.len() as u32;
        account_metas.append(input_metas);
        account_pubkeys.append(input_pubkeys);

        *cursor += encoded_data.len() as u64;
        assert_eq!(input_metas.len(), 0);
        assert_eq!(input_pubkeys.len(), 0);

        Ok(())
    }

    fn write_stored_account_meta<T: TieredAccountMeta>(
        &self,
        account: &StoredAccountMeta,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        account_metas: &mut Vec<T>,
        account_pubkeys: &mut Vec<Pubkey>,
        owners_table: &mut AccountOwnerTable,
        mut data_block: ByteBlockWriter,
        buffered_account_metas: &mut Vec<T>,
        buffered_account_pubkeys: &mut Vec<Pubkey>,
        _hash: &mut Hash,
        account_index_entries: &mut Vec<AccountIndexWriterEntry>,
    ) -> TieredStorageResult<ByteBlockWriter> {
        if !account.sanitize() {
            // Not Ok
        }

        let optional_fields = AccountMetaOptionalFields {
            rent_epoch: Some(account.rent_epoch()),
            account_hash: Some(*account.hash()),
            write_version: Some(account.write_version()),
        };

        let account_raw_size =
            std::mem::size_of::<T>() + account.data_len() as usize + optional_fields.size();

        if !T::supports_shared_account_block() || account_raw_size > ACCOUNT_DATA_BLOCK_SIZE {
            account_index_entries.push(self.write_blob_account_block(
                cursor,
                footer,
                account_metas,
                account_pubkeys,
                owners_table,
                account,
            )?);
            return Ok(data_block);
        }

        // If the current data cannot fit in the current block, then
        // persist the current block.

        if data_block.raw_len() + account_raw_size > ACCOUNT_DATA_BLOCK_SIZE {
            self.flush_account_block(
                cursor,
                footer,
                account_metas,
                account_pubkeys,
                buffered_account_metas,
                buffered_account_pubkeys,
                data_block,
            )?;
            data_block = self.new_data_block_writer(footer);
        }

        let owner_index = owners_table.check_and_add(account.owner());
        let local_offset = data_block.raw_len();

        account_index_entries.push(AccountIndexWriterEntry {
            pubkey: *account.pubkey(),
            block_offset: *cursor,
            intra_block_offset: local_offset as u64,
        });

        let mut flags = AccountMetaFlags::new_from(&optional_fields);
        flags.set_executable(account.executable());
        let meta = T::new()
            .with_lamports(account.lamports())
            .with_owner_index(owner_index)
            .with_account_data_size(account.data_len())
            .with_account_data_padding(((8 - (account.data_len() % 8)) % 8).try_into().unwrap())
            .with_flags(&flags);

        // COMMENT(yhchiang): MetaAndData
        {
            data_block.write_type(&meta)?;
        }

        data_block.write(account.data())?;
        data_block.write_optional_fields(&optional_fields)?;

        buffered_account_metas.push(meta);
        buffered_account_pubkeys.push(*account.pubkey());

        Ok(data_block)
    }

    fn write_blob_account_block<T: TieredAccountMeta>(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        account_metas: &mut Vec<T>,
        account_pubkeys: &mut Vec<Pubkey>,
        owners_table: &mut AccountOwnerTable,
        account: &StoredAccountMeta,
    ) -> TieredStorageResult<AccountIndexWriterEntry> {
        let owner_index = owners_table.check_and_add(account.owner());
        let optional_fields = AccountMetaOptionalFields {
            rent_epoch: Some(account.rent_epoch()),
            account_hash: Some(*account.hash()),
            write_version: Some(account.write_version()),
        };

        let index_entry = AccountIndexWriterEntry {
            pubkey: *account.pubkey(),
            block_offset: *cursor,
            intra_block_offset: 0,
        };

        let mut flags = AccountMetaFlags::new_from(&optional_fields);
        flags.set_executable(account.executable());
        let meta = T::new()
            .with_lamports(account.lamports())
            .with_owner_index(owner_index)
            .with_account_data_size(account.data_len())
            .with_account_data_padding(((8 - (account.data_len() % 8)) % 8).try_into().unwrap())
            .with_flags(&flags);

        let mut writer = ByteBlockWriter::new(footer.account_block_format);
        // COMMENT(yhchiang): MetaAndData
        {
            writer.write_type(&meta)?;
        }

        writer.write(account.data())?;
        if meta.account_data_padding() > 0 {
            let padding = [0u8; 8];
            writer.write(&padding[0..meta.account_data_padding() as usize])?;
        }
        writer.write_optional_fields(&optional_fields)?;

        let data = writer.finish().unwrap();
        let compressed_length = data.len();
        self.storage.write_bytes(&data)?;

        account_metas.push(meta);
        account_pubkeys.push(*account.pubkey());

        *cursor += compressed_length as u64;
        footer.account_entry_count += 1;

        Ok(index_entry)
    }

    #[allow(dead_code)]
    pub fn write_from_append_vec(&self, append_vec: &AppendVec) -> TieredStorageResult<()> {
        let mut footer = TieredStorageFooter::default();
        // TODO(yhchiang): make it configerable
        footer.account_meta_format = self.format.account_meta_format.clone();
        footer.account_block_format = self.format.account_block_format.clone();
        footer.format_version = ACCOUNTS_DATA_STORAGE_FORMAT_VERSION;
        let mut cursor = 0;
        let mut account_pubkeys: Vec<Pubkey> = vec![];
        let mut owners_table = AccountOwnerTable::new();
        let mut hash: Hash = Hash::new_unique();
        let mut account_index_entries = Vec::<AccountIndexWriterEntry>::new();

        match footer.account_meta_format {
            AccountMetaFormat::Hot => {
                let mut account_metas = Vec::<HotAccountMeta>::new();
                self.write_account_blocks(
                    &mut cursor,
                    &mut footer,
                    &mut account_metas,
                    &mut account_pubkeys,
                    &mut owners_table,
                    &mut hash,
                    &mut account_index_entries,
                    &append_vec,
                )?;
                footer.account_meta_entry_size = std::mem::size_of::<HotAccountMeta>() as u32;
            } /*
              AccountMetaFormat::Cold => {
                  let mut account_metas = Vec::<ColdAccountMeta>::new();
                  self.write_account_blocks(
                      &mut cursor,
                      &mut footer,
                      &mut account_metas,
                      &mut account_pubkeys,
                      &mut owners_table,
                      &mut hash,
                      &mut account_index_entries,
                      &append_vec,
                  )?;
                  footer.account_meta_entry_size = std::mem::size_of::<ColdAccountMeta>() as u32;
              }*/
        }
        self.write_account_pubkeys_block(&mut cursor, &mut footer, &account_index_entries)?;
        self.write_owners_block(&mut cursor, &mut footer, &owners_table.owners_vec)?;

        assert_eq!(
            self.format.meta_entry_size as u32,
            footer.account_meta_entry_size
        );
        footer.write_footer_block(&self.storage)?;

        Ok(())
    }

    fn write_account_blocks<T: TieredAccountMeta>(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        account_metas: &mut Vec<T>,
        account_pubkeys: &mut Vec<Pubkey>,
        owners_table: &mut AccountOwnerTable,
        // TODO(yhchiang): update hash
        _hash: &mut Hash,
        account_index_entries: &mut Vec<AccountIndexWriterEntry>,
        append_vec: &AppendVec,
    ) -> TieredStorageResult<()> {
        let mut offset = 0;
        footer.account_block_size = ACCOUNT_DATA_BLOCK_SIZE as u64;

        let mut buffered_account_metas = Vec::<T>::new();
        let mut buffered_account_pubkeys: Vec<Pubkey> = vec![];
        let mut data_block_writer = self.new_data_block_writer(footer);

        while let Some((account, next_offset)) = append_vec.get_account(offset) {
            offset = next_offset;
            data_block_writer = self.write_stored_account_meta(
                &account,
                cursor,
                footer,
                account_metas,
                account_pubkeys,
                owners_table,
                data_block_writer,
                &mut buffered_account_metas,
                &mut buffered_account_pubkeys,
                _hash,
                account_index_entries,
            )?;
        }

        // Persist the last block if any
        if buffered_account_metas.len() > 0 {
            self.flush_account_block(
                cursor,
                footer,
                account_metas,
                account_pubkeys,
                &mut buffered_account_metas,
                &mut buffered_account_pubkeys,
                data_block_writer,
            )?;
        }

        assert_eq!(buffered_account_metas.len(), 0);
        assert_eq!(buffered_account_pubkeys.len(), 0);
        assert_eq!(footer.account_entry_count, account_metas.len() as u32);

        Ok(())
    }
}
