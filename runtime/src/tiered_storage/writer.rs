//! docs/src/proposals/append-vec-storage.md

use {
    crate::{
        account_storage::meta::{
            StorableAccountsWithHashesAndWriteVersions, StoredMetaWriteVersion,
            StoredAccountInfo,
        },
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        storable_accounts::StorableAccounts,
        tiered_storage::{
            cold::ColdAccountMeta,
            data_block::AccountBlockWriter,
            file::TieredStorageFile,
            footer::{AccountMetaFormat, TieredFileFormat, TieredStorageFooter},
            hot::HotAccountMeta,
            index::{AccountIndexWriterEntry2, HotAccountIndexer},
            meta::{AccountMetaFlags, AccountMetaOptionalFields, TieredAccountMeta},
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

    fn append_accounts_impl2<
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
        let mut account_pubkeys: Vec<&Pubkey> = vec![];
        let mut owners_table = AccountOwnerTable::new();
        let mut dummy_hash: Hash = Hash::new_unique();

        let mut data_block_writer = self.new_data_block_writer(&footer);
        footer.account_block_size = ACCOUNT_DATA_BLOCK_SIZE as u64;
        footer.account_meta_entry_size = std::mem::size_of::<W>() as u32;

        let mut buffered_account_metas = Vec::<W>::new();
        let mut buffered_account_pubkeys: Vec<&Pubkey> = vec![];

        let len = accounts.accounts.len();
        let mut input_pubkey_map: HashMap<&Pubkey, usize> = HashMap::with_capacity(len);
        let mut account_index_entries = Vec::<AccountIndexWriterEntry2>::new();

        for i in skip..len {
            let (account, pubkey, hash, write_version) = accounts.get(i);
            input_pubkey_map.insert(pubkey, i);

            data_block_writer = self
                .write_single_account(
                    account.unwrap(),
                    pubkey,
                    hash,
                    write_version,
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
            self.flush_account_block2(
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

        self.write_account_pubkeys_block2(&mut cursor, &mut footer, &account_index_entries)
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
            AccountMetaFormat::Cold => info!(
                "[Cold] append_accounts successfully completed. Footer: {:?}",
                footer
            ),
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
                self.append_accounts_impl2(accounts, footer, Vec::<HotAccountMeta>::new(), skip)
            }
            AccountMetaFormat::Cold => {
                info!(
                    "[Cold] Appending {} accounts to cold storage.",
                    accounts.len() - skip
                );
                self.append_accounts_impl2(accounts, footer, Vec::<ColdAccountMeta>::new(), skip)
            }
        }
    }

    fn new_data_block_writer(&self, footer: &TieredStorageFooter) -> AccountBlockWriter {
        return AccountBlockWriter::new(footer.account_block_format);
    }

    pub(crate) fn write_account_pubkeys_block2(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        index_entries: &Vec<AccountIndexWriterEntry2>,
    ) -> std::io::Result<()> {
        footer.account_index_offset = *cursor;
        match footer.account_meta_format {
            AccountMetaFormat::Hot => {
                *cursor += HotAccountIndexer::write_index_block2(&self.storage, index_entries)?;
            }
            AccountMetaFormat::Cold => unimplemented!(),
        }
        Ok(())
    }

    fn write_owners_block(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        pubkeys: &Vec<Pubkey>,
    ) -> std::io::Result<()> {
        footer.owners_offset = *cursor;
        footer.owner_count = pubkeys.len() as u32;
        footer.owner_entry_size = mem::size_of::<Pubkey>() as u32;

        self.write_pubkeys_block(cursor, pubkeys)
    }

    fn write_pubkeys_block(&self, cursor: &mut u64, pubkeys: &Vec<Pubkey>) -> std::io::Result<()> {
        for pubkey in pubkeys {
            *cursor += self.storage.write_type(pubkey)? as u64;
        }

        Ok(())
    }

    fn flush_account_block2<'a, T: TieredAccountMeta>(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        account_metas: &mut Vec<T>,
        account_pubkeys: &mut Vec<&'a Pubkey>,
        input_metas: &mut Vec<T>,
        input_pubkeys: &mut Vec<&'a Pubkey>,
        data_block_writer: AccountBlockWriter,
    ) -> std::io::Result<()> {
        // Persist the current block
        let (encoded_data, _raw_data_size) = data_block_writer.finish()?;
        self.storage.write_bytes(&encoded_data)?;

        assert_eq!(input_metas.len(), input_pubkeys.len());

        for input_meta in &mut input_metas.into_iter() {
            input_meta.set_block_offset(*cursor);
        }
        for input_meta in &mut input_metas.into_iter() {
            assert_eq!(input_meta.block_offset(), *cursor);
        }
        footer.account_entry_count += input_metas.len() as u32;
        account_metas.append(input_metas);
        account_pubkeys.append(input_pubkeys);

        *cursor += encoded_data.len() as u64;
        assert_eq!(input_metas.len(), 0);
        assert_eq!(input_pubkeys.len(), 0);

        Ok(())
    }

    fn write_single_account<'a, T: TieredAccountMeta>(
        &self,
        account: &(impl ReadableAccount + Sync),
        address: &'a Pubkey,
        hash: &Hash,
        write_version: StoredMetaWriteVersion,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        account_metas: &mut Vec<T>,
        account_pubkeys: &mut Vec<&'a Pubkey>,
        owners_table: &mut AccountOwnerTable,
        mut data_block: AccountBlockWriter,
        buffered_account_metas: &mut Vec<T>,
        buffered_account_pubkeys: &mut Vec<&'a Pubkey>,
        _hash: &mut Hash,
        account_index_entries: &mut Vec<AccountIndexWriterEntry2<'a>>,
    ) -> std::io::Result<AccountBlockWriter> {
        let optional_fields = AccountMetaOptionalFields {
            rent_epoch:
                (account.rent_epoch() != u64::MAX).then(|| account.rent_epoch()),
            account_hash:
                (*hash != Hash::default()).then(|| *hash),
            // TODO(yhchiang): free to kill the write_version
            write_version_obsolete:
                (write_version != u64::MAX).then(|| write_version),
        };

        let account_raw_size =
            std::mem::size_of::<T>() + account.data().len() + optional_fields.size();

        if T::is_blob_account_data(account_raw_size as u64) {
            account_index_entries.push(self.write_blob_account_block2(
                cursor,
                footer,
                account_metas,
                account_pubkeys,
                owners_table,
                account,
                address,
                hash,
                write_version,
            )?);
            return Ok(data_block);
        }

        // If the current data cannot fit in the current block, then
        // persist the current block.
        if data_block.len() + account_raw_size > ACCOUNT_DATA_BLOCK_SIZE {
            self.flush_account_block2(
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

        let owner_local_id = owners_table.check_and_add(account.owner());
        let local_offset = data_block.len();

        account_index_entries.push(AccountIndexWriterEntry2 {
            pubkey: address,
            block_offset: *cursor,
            intra_block_offset: local_offset as u64,
        });

        let mut meta = T::new();
        meta.with_lamports(account.lamports())
            .with_block_offset(*cursor)
            .with_owner_local_id(owner_local_id)
            .with_uncompressed_data_size(account.data().len() as u64)
            .with_intra_block_offset(local_offset as u16)
            .with_flags(
                AccountMetaFlags::new()
                    .with_bit(AccountMetaFlags::EXECUTABLE, account.executable())
                    .to_value(),
            )
            .with_optional_fields(&optional_fields);

        // COMMENT(yhchiang): MetaAndData
        {
            data_block.write_type(&meta)?;
        }

        data_block.write(account.data(), account.data().len())?;
        optional_fields.write(&mut data_block)?;

        buffered_account_metas.push(meta);
        buffered_account_pubkeys.push(address);

        Ok(data_block)
    }

    fn write_blob_account_block2<'a, T: TieredAccountMeta>(
        &self,
        cursor: &mut u64,
        footer: &mut TieredStorageFooter,
        account_metas: &mut Vec<T>,
        account_pubkeys: &mut Vec<&'a Pubkey>,
        owners_table: &mut AccountOwnerTable,
        account: &(impl ReadableAccount + Sync),
        address: &'a Pubkey,
        hash: &Hash,
        write_version: StoredMetaWriteVersion,
    ) -> std::io::Result<AccountIndexWriterEntry2<'a>> {
        let owner_local_id = owners_table.check_and_add(account.owner());
        // TODO(yhchiang): convert it to OptionalFieldsWriterEntry
        let optional_fields = AccountMetaOptionalFields {
            rent_epoch:
                (account.rent_epoch() != u64::MAX).then(|| account.rent_epoch()),
            account_hash:
                (*hash != Hash::default()).then(|| *hash),
            write_version_obsolete:
                (write_version != u64::MAX).then(|| write_version),
        };

        let index_entry = AccountIndexWriterEntry2 {
            pubkey: address,
            block_offset: *cursor,
            intra_block_offset: 0,
        };

        let mut meta = T::new();
        meta.with_lamports(account.lamports())
            .with_block_offset(*cursor)
            .with_owner_local_id(owner_local_id)
            .with_uncompressed_data_size(account.data().len() as u64)
            .with_intra_block_offset(0)
            .with_flags(
                AccountMetaFlags::new()
                    .with_bit(AccountMetaFlags::EXECUTABLE, account.executable())
                    .to_value(),
            )
            .with_optional_fields(&optional_fields);

        let mut writer = AccountBlockWriter::new(footer.account_block_format);
        // COMMENT(yhchiang): MetaAndData
        {
            writer.write_type(&meta)?;
        }

        writer.write(account.data(), account.data().len())?;
        if meta.padding_bytes() > 0 {
            let padding = [0u8; 8];
            writer.write(&padding, meta.padding_bytes() as usize)?;
        }
        optional_fields.write(&mut writer)?;

        let (data, _uncompressed_len) = writer.finish().unwrap();
        let compressed_length = data.len();
        self.storage.write_bytes(&data)?;

        account_metas.push(meta);
        account_pubkeys.push(address);

        *cursor += compressed_length as u64;
        footer.account_entry_count += 1;

        Ok(index_entry)
    }
}
