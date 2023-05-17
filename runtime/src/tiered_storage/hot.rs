#![allow(unused_imports)]
use {
    crate::{
        account_storage::meta::{StoredAccountMeta, StoredMetaWriteVersion},
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        append_vec::MatchAccountOwnerError,
        tiered_storage::{
            data_block::AccountBlock,
            error::TieredStorageResult,
            file::TieredStorageFile,
            footer::{
                AccountBlockFormat, AccountIndexFormat, AccountMetaFormat, OwnersBlockFormat,
                TieredFileFormat, TieredStorageFooter, TieredStorageMagicNumber,
                FOOTER_MAGIC_NUMBER, FOOTER_TAIL_SIZE,
            },
            index::HotAccountIndexer,
            meta::{
                get_compressed_block_size, AccountMetaFlags, AccountMetaOptionalFields,
                TieredAccountMeta, ACCOUNT_DATA_ENTIRE_BLOCK, DEFAULT_ACCOUNT_HASH,
            },
            mmap_utils::{get_slice, get_type},
            reader::{TieredStorageReader, TieredStoredAccountMeta},
        },
        u64_align,
    },
    log::*,
    memmap2::{Mmap, MmapOptions},
    solana_sdk::{hash::Hash, pubkey::Pubkey, stake_history::Epoch},
    std::{collections::HashMap, fs::OpenOptions, mem::size_of, option::Option, path::Path},
};

const OWNER_INDEX_MASK: u32 = 0x1fff_ffff;
const CLEAR_BLOCK_PADDING_MASK: u32 = OWNER_INDEX_MASK;
const BLOCK_PADDING_MASK: u32 = 0xe000_0000;
const BLOCK_PADDING_SHIFT: u32 = 29;

pub static HOT_FORMAT: TieredFileFormat = TieredFileFormat {
    meta_entry_size: std::mem::size_of::<HotAccountMeta>(),
    account_meta_format: AccountMetaFormat::Hot,
    owners_block_format: OwnersBlockFormat::LocalIndex,
    account_index_format: AccountIndexFormat::Linear,
    account_block_format: AccountBlockFormat::AlignedRaw,
};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct HotAccountMeta {
    lamports: u64,
    // the high 8-bits are used to store padding and data block
    // format information.
    // Use block_offset() to obtain the actual block offset.
    // block_offset_info: u64,
    padding_and_owner_index: u32,
    flags: u32,
}

impl HotAccountMeta {
    #[allow(dead_code)]
    fn new_from_file(ads_file: &TieredStorageFile) -> std::io::Result<Self> {
        let mut entry = HotAccountMeta::new();
        ads_file.read_type(&mut entry)?;

        Ok(entry)
    }

    fn set_padding_bytes(&mut self, paddings: u8) {
        assert!(paddings <= 7);
        self.padding_and_owner_index &= CLEAR_BLOCK_PADDING_MASK;
        self.padding_and_owner_index |= (paddings as u32) << BLOCK_PADDING_SHIFT;
        // self.block_offset_info &= CLEAR_PADDINGS_MASK;
        // self.block_offset_info |= (paddings as u64) << PADDINGS_SHIFT;
    }

    fn get_type<'a, T>(data_block: &'a [u8], offset: usize) -> &'a T {
        unsafe {
            let raw_ptr = std::slice::from_raw_parts(
                data_block[offset..offset + std::mem::size_of::<T>()].as_ptr() as *const u8,
                std::mem::size_of::<T>(),
            );
            let ptr: *const T = raw_ptr.as_ptr() as *const T;
            return &*ptr;
        }
    }
}

impl TieredAccountMeta for HotAccountMeta {
    fn new() -> Self {
        HotAccountMeta {
            lamports: 0,
            // block_offset_info: 0,
            // owner_index: 0,
            padding_and_owner_index: 0,
            flags: 0,
        }
    }

    fn is_blob_account_data(_data_len: u64) -> bool {
        true
    }

    fn lamports(&self) -> u64 {
        self.lamports
    }

    fn with_lamports(&mut self, lamports: u64) -> &mut Self {
        self.lamports = lamports;
        self
    }

    fn with_block_offset(&mut self, offset: u64) -> &mut Self {
        self.set_block_offset(offset);
        self
    }

    fn with_data_tailing_paddings(&mut self, paddings: u8) -> &mut Self {
        self.set_padding_bytes(paddings);
        self
    }

    fn with_owner_local_id(&mut self, owner_index: u32) -> &mut Self {
        assert!(owner_index <= OWNER_INDEX_MASK);
        self.padding_and_owner_index |= owner_index & OWNER_INDEX_MASK;
        self
    }

    fn with_uncompressed_data_size(&mut self, data_size: u64) -> &mut Self {
        // Hot meta derives its data length by comparing two consecutive offsets.
        // TODO(yhchiang): invoke with_paddings() here.
        self.set_padding_bytes(((8 - (data_size % 8)) % 8) as u8);
        self
    }

    fn with_intra_block_offset(&mut self, _offset: u16) -> &mut Self {
        // hot meta always have intra block offset equals to 0 except
        // its block_offset_info indocates it is inside a shared block.
        self
    }

    fn with_optional_fields(&mut self, fields: &AccountMetaOptionalFields) -> &mut Self {
        fields.update_flags(&mut self.flags);
        self
    }

    fn with_flags(&mut self, flags: u32) -> &mut Self {
        self.flags = flags;
        self
    }

    fn block_offset(&self) -> u64 {
        0
    }

    fn padding_bytes(&self) -> u8 {
        ((self.padding_and_owner_index & BLOCK_PADDING_MASK) >> BLOCK_PADDING_SHIFT)
            .try_into()
            .unwrap()
    }

    fn set_block_offset(&mut self, _offset: u64) {}

    fn intra_block_offset(&self) -> u16 {
        0
    }

    fn owner_local_id(&self) -> u32 {
        self.padding_and_owner_index & OWNER_INDEX_MASK
    }

    fn flags_get(&self, bit_field: u32) -> bool {
        AccountMetaFlags::get(&self.flags, bit_field)
    }

    fn rent_epoch(&self, data_block: &[u8]) -> Option<Epoch> {
        let offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            let epoch: Epoch = *Self::get_type(data_block, offset);
            return Some(epoch);
        }
        None
    }

    fn account_hash<'a>(&self, data_block: &'a [u8]) -> &'a Hash {
        let mut offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            offset += std::mem::size_of::<Epoch>();
        }
        if self.flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH) {
            return Self::get_type(data_block, offset);
        }
        return &DEFAULT_ACCOUNT_HASH;
    }

    fn write_version(&self, data_block: &[u8]) -> Option<StoredMetaWriteVersion> {
        let mut offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            offset += std::mem::size_of::<Epoch>();
        }
        if self.flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH) {
            offset += std::mem::size_of::<Hash>();
        }
        if self.flags_get(AccountMetaFlags::HAS_WRITE_VERSION) {
            let write_version: StoredMetaWriteVersion = *Self::get_type(data_block, offset);
            return Some(write_version);
        }
        None
    }

    fn data_len(&self, data_block: &[u8]) -> usize {
        self.optional_fields_offset(data_block)
            .saturating_sub(self.padding_bytes() as usize)
    }

    fn optional_fields_offset<'a>(&self, data_block: &'a [u8]) -> usize {
        data_block.len().saturating_sub(self.optional_fields_size())
    }

    fn account_data<'a>(&self, data_block: &'a [u8]) -> &'a [u8] {
        &data_block[0..self.data_len(data_block)]
    }

    fn is_blob_account(&self) -> bool {
        todo!();
    }

    fn write_account_meta_entry(&self, ads_file: &TieredStorageFile) -> std::io::Result<usize> {
        ads_file.write_type(self)?;

        Ok(std::mem::size_of::<HotAccountMeta>())
    }

    fn stored_size(
        _footer: &TieredStorageFooter,
        _metas: &Vec<impl TieredAccountMeta>,
        _i: usize,
    ) -> usize {
        // TODO(yhchiang): need a new way to obtain data size
        std::mem::size_of::<HotAccountMeta>()
    }
}

#[derive(Debug)]
pub struct HotStorageReader {
    map: Mmap,
    footer: TieredStorageFooter,
}

impl HotStorageReader {
    pub fn new_from_path<P: AsRef<Path>>(path: P) -> TieredStorageResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(false)
            .open(path.as_ref())?;
        let map = unsafe { MmapOptions::new().map(&file)? };
        let footer = TieredStorageFooter::new_from_mmap(&map)?.clone();
        assert!(map.len() > 0);
        info!(
            "[Hot] Opening hot storage from {:?} with mmap length {}.  Footer: {:?}",
            path.as_ref().display(),
            map.len(),
            footer
        );

        Ok(Self { map, footer })
    }

    pub fn footer(&self) -> &TieredStorageFooter {
        &self.footer
    }

    pub fn num_accounts(&self) -> usize {
        self.footer.account_entry_count as usize
    }

    pub fn account_matches_owners(
        &self,
        multiplied_index: usize,
        owners: &[&Pubkey],
    ) -> Result<usize, MatchAccountOwnerError> {
        let index = Self::multiplied_index_to_index(multiplied_index);
        if index >= self.num_accounts() {
            return Err(MatchAccountOwnerError::UnableToLoad);
        }

        let owner = self.get_owner_address(index).unwrap();
        owners
            .iter()
            .position(|entry| &owner == entry)
            .ok_or(MatchAccountOwnerError::NoMatch)
    }

    fn multiplied_index_to_index(multiplied_index: usize) -> usize {
        // This is a temporary workaround to work with existing AccountInfo
        // implementation that ties to AppendVec with the assumption that the offset
        // is a multiple of ALIGN_BOUNDARY_OFFSET, while tiered storage actually talks
        // about index instead of offset.
        multiplied_index / ALIGN_BOUNDARY_OFFSET
    }

    fn get_account_meta<'a>(&'a self, index: usize) -> std::io::Result<&'a HotAccountMeta> {
        // COMMENT(yhchiang): MetaAndData
        let offset = HotAccountIndexer::get_meta_offset(&self.map, &self.footer, index)? as usize;
        self.get_account_meta_from_offset(offset)
    }

    fn get_account_meta_from_offset<'a>(
        &'a self,
        offset: usize,
    ) -> std::io::Result<&'a HotAccountMeta> {
        let (meta, _): (&'a HotAccountMeta, _) = get_type(&self.map, offset as usize)?;
        Ok(meta)
    }

    fn get_account_address<'a>(&'a self, index: usize) -> std::io::Result<&'a Pubkey> {
        let offset = HotAccountIndexer::get_pubkey_offset(&self.footer, index);
        // let offset =
        //    self.footer.account_index_offset as usize + (std::mem::size_of::<Pubkey>() * index);
        let (pubkey, _): (&'a Pubkey, _) = get_type(&self.map, offset)?;
        Ok(pubkey)
    }

    fn get_owner_address<'a>(&'a self, index: usize) -> std::io::Result<&'a Pubkey> {
        let meta = self.get_account_meta(index)?;
        let offset = self.footer.owners_offset as usize
            + (std::mem::size_of::<Pubkey>() * (meta.owner_local_id() as usize));
        let (pubkey, _): (&'a Pubkey, _) = get_type(&self.map, offset)?;
        Ok(pubkey)
    }

    fn get_data_block_size(&self, meta_offset: usize, index: usize) -> usize {
        if (index + 1) as u32 == self.footer.account_entry_count {
            assert!(self.footer.account_index_offset as usize > meta_offset);
            return self.footer.account_index_offset as usize
                - meta_offset
                - std::mem::size_of::<HotAccountMeta>();
        }

        let next_meta_offset =
            HotAccountIndexer::get_meta_offset(&self.map, &self.footer, index + 1).unwrap() as usize;

        next_meta_offset
            .saturating_sub(meta_offset)
            .saturating_sub(std::mem::size_of::<HotAccountMeta>())
    }

    fn get_data_block<'a>(&'a self, meta_offset: usize, index: usize) -> std::io::Result<&'a [u8]> {
        let (data, _): (&'a [u8], _) = get_slice(
            &self.map,
            meta_offset + std::mem::size_of::<HotAccountMeta>(),
            self.get_data_block_size(meta_offset, index),
        )?;
        Ok(data)
    }

    pub fn get_account<'a>(
        &'a self,
        multiplied_index: usize,
    ) -> Option<(StoredAccountMeta<'a>, usize)> {
        let index = Self::multiplied_index_to_index(multiplied_index);
        // TODO(yhchiang): remove this TODO
        // TODO2
        if index >= self.footer.account_entry_count as usize {
            return None;
        }

        let meta_offset =
            HotAccountIndexer::get_meta_offset(&self.map, &self.footer, index).unwrap() as usize;
        let meta: &'a HotAccountMeta = self.get_account_meta_from_offset(meta_offset).unwrap();
        let address: &'a Pubkey = self.get_account_address(index).unwrap();
        let owner: &'a Pubkey = self.get_owner_address(index).unwrap();
        let data_block: &'a [u8] = self.get_data_block(meta_offset, index).unwrap();

        return Some((
            StoredAccountMeta::Hot(TieredStoredAccountMeta {
                meta: meta,
                pubkey: address,
                owner: owner,
                index: multiplied_index,
                data_block: data_block,
            }),
            multiplied_index + ALIGN_BOUNDARY_OFFSET,
        ));
    }
}

#[cfg(test)]
pub mod tests {
    use {
        crate::{
            account_storage::meta::StoredMetaWriteVersion,
            append_vec::test_utils::get_append_vec_path,
            tiered_storage::{
                file::TieredStorageFile,
                footer::{
                    AccountBlockFormat, AccountIndexFormat, AccountMetaFormat,
                    OwnersBlockFormat, TieredStorageFooter, FOOTER_SIZE,
                },
                hot::{HotAccountMeta, HotStorageReader},
                meta::{AccountMetaFlags, AccountMetaOptionalFields, TieredAccountMeta},
            },
        },
        ::solana_sdk::{hash::Hash, pubkey::Pubkey, stake_history::Epoch},
        memoffset::offset_of,
        std::mem::size_of,
    };

    #[test]
    fn test_hot_account_meta_layout() {
        assert_eq!(offset_of!(HotAccountMeta, lamports), 0x00);
        // assert_eq!(offset_of!(HotAccountMeta, block_offset_info), 0x08);
        assert_eq!(offset_of!(HotAccountMeta, padding_and_owner_index), 0x08);
        assert_eq!(offset_of!(HotAccountMeta, flags), 0x0C);
        assert_eq!(std::mem::size_of::<HotAccountMeta>(), 16);
    }

    #[test]
    fn test_hot_offset_and_padding() {
        let owner_index: u32 = 0x1fff_ef98;
        let length: u64 = 153233;
        let mut hot_meta = HotAccountMeta::new();
        hot_meta
            .with_owner_local_id(owner_index)
            .with_uncompressed_data_size(length);
        assert_eq!(hot_meta.owner_local_id(), owner_index);
        assert_eq!(hot_meta.padding_bytes(), ((8 - (length % 8)) % 8) as u8);
    }

    #[test]
    fn test_hot_account_meta() {
        let path = get_append_vec_path("test_hot_account_meta");

        const TEST_LAMPORT: u64 = 2314232137;
        const BLOCK_OFFSET: u64 = 56987;
        const PADDINGS: u8 = 5;
        const OWNER_LOCAL_ID: u32 = 0x1fef_1234;
        const TEST_RENT_EPOCH: Epoch = 7;
        const TEST_WRITE_VERSION: StoredMetaWriteVersion = 0;

        let optional_fields = AccountMetaOptionalFields {
            rent_epoch: Some(TEST_RENT_EPOCH),
            account_hash: Some(Hash::new_unique()),
            write_version_obsolete: Some(TEST_WRITE_VERSION),
        };

        let mut expected_entry = HotAccountMeta::new();
        expected_entry
            .with_lamports(TEST_LAMPORT)
            .with_block_offset(BLOCK_OFFSET)
            .with_data_tailing_paddings(PADDINGS)
            .with_owner_local_id(OWNER_LOCAL_ID)
            .with_flags(
                AccountMetaFlags::new()
                    .with_bit(AccountMetaFlags::EXECUTABLE, true)
                    .to_value(),
            )
            .with_optional_fields(&optional_fields);

        {
            let mut ads_file = TieredStorageFile::new_writable(&path.path);
            expected_entry
                .write_account_meta_entry(&mut ads_file)
                .unwrap();
        }

        let mut ads_file = TieredStorageFile::new_readonly(&path.path);
        let entry = HotAccountMeta::new_from_file(&mut ads_file).unwrap();

        assert_eq!(expected_entry, entry);
        assert_eq!(entry.flags_get(AccountMetaFlags::EXECUTABLE), true);
        assert_eq!(entry.flags_get(AccountMetaFlags::HAS_RENT_EPOCH), true);
    }

    #[test]
    fn test_max_owner_index_and_padding() {
        let owner_index: u32 = 0x1fff_ffff;
        let paddings: u8 = 7;
        let mut hot_meta = HotAccountMeta::new();
        hot_meta.with_owner_local_id(owner_index);
        hot_meta.set_padding_bytes(paddings);
        assert_eq!(hot_meta.owner_local_id(), owner_index);
        assert_eq!(hot_meta.padding_bytes(), paddings);
    }

    #[test]
    fn test_hot_storage_footer() {
        let path = get_append_vec_path("test_hot_storage_footer");
        let expected_footer = TieredStorageFooter {
            account_meta_format: AccountMetaFormat::Hot,
            owners_block_format: OwnersBlockFormat::LocalIndex,
            account_index_format: AccountIndexFormat::Linear,
            account_block_format: AccountBlockFormat::AlignedRaw,
            account_entry_count: 300,
            account_meta_entry_size: 16,
            account_block_size: 4096,
            owner_count: 250,
            owner_entry_size: 32,
            account_index_offset: 1069600,
            owners_offset: 1081200,
            hash: Hash::new_unique(),
            min_account_address: Pubkey::default(),
            max_account_address: Pubkey::new_unique(),
            footer_size: FOOTER_SIZE as u64,
            format_version: 1,
        };

        {
            let ads_file = TieredStorageFile::new_writable(&path.path);
            expected_footer.write_footer_block(&ads_file).unwrap();
        }

        // Reopen the same storage, and expect the persisted footer is
        // the same as what we have written.
        {
            let hot_storage = HotStorageReader::new_from_path(&path.path).unwrap();
            assert_eq!(expected_footer, *hot_storage.footer());
        }
    }
}
