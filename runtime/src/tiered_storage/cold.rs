use {
    crate::{
        account_storage::meta::{StoredAccountMeta, StoredMetaWriteVersion},
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        append_vec::MatchAccountOwnerError,
        tiered_storage::{
            data_block::{AccountDataBlock, AccountDataBlockFormat},
            file::TieredStorageFile,
            footer::TieredStorageFooter,
            meta_entries::{
                AccountMetaFlags, AccountMetaOptionalFields, TieredAccountMeta,
                ACCOUNT_DATA_ENTIRE_BLOCK,
            },
            reader::{TieredStorageReader, TieredStoredAccountMeta},
        },
    },
    solana_sdk::{hash::Hash, pubkey::Pubkey, stake_history::Epoch},
    std::{collections::HashMap, mem::size_of, path::Path},
};

lazy_static! {
    pub static ref DEFAULT_ACCOUNT_HASH: Hash = Hash::default();
}

#[derive(Debug)]
pub struct ColdStorageReader {
    pub(crate) footer: TieredStorageFooter,
    pub(crate) metas: Vec<ColdAccountMeta>,
    accounts: Vec<Pubkey>,
    owners: Vec<Pubkey>,
    data_blocks: HashMap<u64, Vec<u8>>,
}

impl ColdStorageReader {
    pub fn new_from_file(file_path: impl AsRef<Path>) -> std::io::Result<TieredStorageReader> {
        let storage = TieredStorageFile::new(file_path, false /* create */);
        let footer = ColdReaderBuilder::read_footer_block(&storage)?;

        let metas = ColdReaderBuilder::read_account_metas_block(&storage, &footer)?;
        let accounts = ColdReaderBuilder::read_account_addresses_block(&storage, &footer)?;
        let owners = ColdReaderBuilder::read_owners_block(&storage, &footer)?;
        let data_blocks = ColdReaderBuilder::read_data_blocks(&storage, &footer, &metas)?;

        Ok(TieredStorageReader::Cold(ColdStorageReader {
            footer,
            metas,
            accounts,
            owners,
            data_blocks,
        }))
    }

    pub fn num_accounts(&self) -> usize {
        self.footer.account_meta_count.try_into().unwrap()
    }

    fn multiplied_index_to_index(multiplied_index: usize) -> usize {
        // This is a temporary workaround to work with existing AccountInfo
        // implementation that ties to AppendVec with the assumption that the offset
        // is a multiple of ALIGN_BOUNDARY_OFFSET, while tiered storage actually talks
        // about index instead of offset.
        multiplied_index / ALIGN_BOUNDARY_OFFSET
    }

    pub fn account_matches_owners(
        &self,
        multiplied_index: usize,
        owners: &[&Pubkey],
    ) -> Result<usize, MatchAccountOwnerError> {
        let index = Self::multiplied_index_to_index(multiplied_index);
        if index >= self.metas.len() {
            return Err(MatchAccountOwnerError::UnableToLoad);
        }

        owners
            .iter()
            .position(|entry| &&self.owners[self.metas[index].owner_local_id() as usize] == entry)
            .ok_or(MatchAccountOwnerError::NoMatch)
    }

    pub fn get_account<'a>(
        &'a self,
        multiplied_index: usize,
    ) -> Option<(StoredAccountMeta<'a>, usize)> {
        let index = Self::multiplied_index_to_index(multiplied_index);
        if index >= self.metas.len() {
            return None;
        }
        if let Some(data_block) = self.data_blocks.get(&self.metas[index].block_offset()) {
            return Some((
                StoredAccountMeta::Tiered(TieredStoredAccountMeta {
                    meta: &self.metas[index],
                    pubkey: &self.accounts[index],
                    owner: &self.owners[self.metas[index].owner_local_id() as usize],
                    index: multiplied_index,
                    data_block: data_block,
                }),
                multiplied_index + ALIGN_BOUNDARY_OFFSET,
            ));
        }
        None
    }
}

pub(crate) struct ColdReaderBuilder {}

impl ColdReaderBuilder {
    fn read_footer_block(storage: &TieredStorageFile) -> std::io::Result<TieredStorageFooter> {
        TieredStorageFooter::new_from_footer_block(&storage)
    }

    fn read_account_metas_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
    ) -> std::io::Result<Vec<ColdAccountMeta>> {
        let mut metas: Vec<ColdAccountMeta> =
            Vec::with_capacity(footer.account_meta_count as usize);

        (&storage).seek(footer.account_metas_offset)?;

        for _ in 0..footer.account_meta_count {
            metas.push(ColdAccountMeta::new_from_file(&storage)?);
        }

        Ok(metas)
    }

    fn read_account_addresses_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
    ) -> std::io::Result<Vec<Pubkey>> {
        Self::read_pubkeys_block(
            storage,
            footer.account_pubkeys_offset,
            footer.account_meta_count,
        )
    }

    fn read_owners_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
    ) -> std::io::Result<Vec<Pubkey>> {
        Self::read_pubkeys_block(storage, footer.owners_offset, footer.owner_count)
    }

    fn read_pubkeys_block(
        storage: &TieredStorageFile,
        offset: u64,
        count: u32,
    ) -> std::io::Result<Vec<Pubkey>> {
        let mut addresses: Vec<Pubkey> = Vec::with_capacity(count as usize);
        (&storage).seek(offset)?;
        for _ in 0..count {
            let mut pubkey = Pubkey::default();
            (&storage).read_type(&mut pubkey)?;
            addresses.push(pubkey);
        }

        Ok(addresses)
    }

    pub fn read_data_blocks(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
        metas: &Vec<ColdAccountMeta>,
    ) -> std::io::Result<HashMap<u64, Vec<u8>>> {
        let count = footer.account_meta_count as usize;
        let mut data_blocks = HashMap::<u64, Vec<u8>>::new();
        for i in 0..count {
            Self::update_data_block_map(&mut data_blocks, storage, footer, metas, i)?;
        }
        Ok(data_blocks)
    }

    fn update_data_block_map(
        data_blocks: &mut HashMap<u64, Vec<u8>>,
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
        metas: &Vec<ColdAccountMeta>,
        index: usize,
    ) -> std::io::Result<()> {
        let block_offset = &metas[index].block_offset();
        if !data_blocks.contains_key(&block_offset) {
            let data_block = Self::read_data_block(storage, footer, metas, index).unwrap();

            data_blocks.insert(metas[index].block_offset(), data_block);
        }
        Ok(())
    }

    pub fn read_data_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
        metas: &Vec<ColdAccountMeta>,
        index: usize,
    ) -> std::io::Result<Vec<u8>> {
        let compressed_block_size = Self::get_compressed_block_size(footer, metas, index) as usize;

        (&storage).seek(metas[index].block_offset())?;

        let mut buffer: Vec<u8> = vec![0; compressed_block_size];
        (&storage).read_bytes(&mut buffer)?;

        // TODO(yhchiang): encoding from footer
        Ok(AccountDataBlock::decode(
            AccountDataBlockFormat::Lz4,
            &buffer[..],
        )?)
    }

    pub(crate) fn get_compressed_block_size(
        footer: &TieredStorageFooter,
        metas: &Vec<ColdAccountMeta>,
        index: usize,
    ) -> usize {
        let mut block_size = footer.account_metas_offset - metas[index].block_offset();

        for i in index..metas.len() {
            if metas[i].block_offset() == metas[index].block_offset() {
                continue;
            }
            block_size = metas[i].block_offset() - metas[index].block_offset();
            break;
        }

        block_size.try_into().unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ColdAccountMeta {
    lamports: u64,
    block_offset: u64,
    uncompressed_data_size: u16,
    intra_block_offset: u16,
    owner_local_id: u32,
    flags: u32,
}

impl TieredAccountMeta for ColdAccountMeta {
    fn lamports(&self) -> u64 {
        self.lamports
    }

    fn block_offset(&self) -> u64 {
        self.block_offset
    }

    fn set_block_offset(&mut self, offset: u64) {
        self.block_offset = offset;
    }

    fn padding_bytes(&self) -> u8 {
        0u8
    }

    fn set_padding_bytes(&mut self, _paddings: u8) {
    }

    fn uncompressed_data_size(&self) -> u16 {
        self.uncompressed_data_size
    }

    fn intra_block_offset(&self) -> u16 {
        self.intra_block_offset
    }

    fn owner_local_id(&self) -> u32 {
        self.owner_local_id
    }

    fn flags_get(&self, bit_field: u32) -> bool {
        AccountMetaFlags::get(&self.flags, bit_field)
    }

    fn rent_epoch(&self, data_block: &[u8]) -> Option<Epoch> {
        let offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            unsafe {
                let unaligned =
                    std::ptr::addr_of!(data_block[offset..offset + std::mem::size_of::<Epoch>()])
                        as *const Epoch;
                return Some(std::ptr::read_unaligned(unaligned));
            }
        }
        None
    }

    fn account_hash<'a>(&self, data_block: &'a [u8]) -> &'a Hash {
        let mut offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            offset += std::mem::size_of::<Epoch>();
        }
        if self.flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH) {
            unsafe {
                let raw_ptr = std::slice::from_raw_parts(
                    data_block[offset..offset + std::mem::size_of::<Hash>()].as_ptr() as *const u8,
                    std::mem::size_of::<Hash>(),
                );
                let ptr: *const Hash = raw_ptr.as_ptr() as *const Hash;
                return &*ptr;
            }
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
            unsafe {
                let unaligned = std::ptr::addr_of!(
                    data_block[offset..offset + std::mem::size_of::<StoredMetaWriteVersion>()]
                ) as *const StoredMetaWriteVersion;
                return Some(std::ptr::read_unaligned(unaligned));
            }
        }
        None
    }

    /*
    fn data_length(&self, data_block: &[u8]) -> Option<u64> {
        let mut offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            offset += std::mem::size_of::<Epoch>();
        }
        if self.flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH) {
            offset += std::mem::size_of::<Hash>();
        }
        if self.flags_get(AccountMetaFlags::HAS_WRITE_VERSION) {
            offset += std::mem::size_of::<StoredMetaWriteVersion>();
        }
        if self.flags_get(AccountMetaFlags::HAS_DATA_LENGTH) {
            unsafe {
                let unaligned =
                    std::ptr::addr_of!(data_block[offset..offset + std::mem::size_of::<u64>()])
                        as *const u64;
                return Some(std::ptr::read_unaligned(unaligned));
            }
        }
        None
    }*/

    fn optional_fields_size(&self) -> usize {
        let mut size_in_bytes = 0;
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            size_in_bytes += size_of::<Epoch>();
        }
        if self.flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH) {
            size_in_bytes += size_of::<Hash>();
        }
        if self.flags_get(AccountMetaFlags::HAS_WRITE_VERSION) {
            size_in_bytes += size_of::<StoredMetaWriteVersion>();
        }
        if self.flags_get(AccountMetaFlags::HAS_DATA_LENGTH) {
            size_in_bytes += size_of::<u64>();
        }

        size_in_bytes
    }

    fn optional_fields_offset<'a>(&self, data_block: &'a [u8]) -> usize {
        if self.is_blob_account() {
            return data_block.len().saturating_sub(self.optional_fields_size());
        }
        (self.intra_block_offset + self.uncompressed_data_size) as usize
    }

    fn account_data<'a>(&self, data_block: &'a [u8]) -> &'a [u8] {
        &data_block[(self.intra_block_offset as usize)..self.optional_fields_offset(data_block)]
    }

    fn is_blob_account(&self) -> bool {
        self.uncompressed_data_size == ACCOUNT_DATA_ENTIRE_BLOCK && self.intra_block_offset == 0
    }

    fn write_account_meta_entry(&self, ads_file: &TieredStorageFile) -> std::io::Result<usize> {
        ads_file.write_type(self)?;

        Ok(std::mem::size_of::<ColdAccountMeta>())
    }
}

impl ColdAccountMeta {
    pub fn new() -> Self {
        Self {
            ..ColdAccountMeta::default()
        }
    }

    pub fn new_from_file(ads_file: &TieredStorageFile) -> std::io::Result<Self> {
        let mut entry = ColdAccountMeta::new();
        ads_file.read_type(&mut entry)?;

        Ok(entry)
    }

    pub fn with_lamports(mut self, lamports: u64) -> Self {
        self.lamports = lamports;
        self
    }

    pub fn with_block_offset(mut self, offset: u64) -> Self {
        self.block_offset = offset;
        self
    }

    pub fn with_owner_local_id(mut self, local_id: u32) -> Self {
        self.owner_local_id = local_id;
        self
    }

    pub fn with_uncompressed_data_size(mut self, data_size: u16) -> Self {
        self.uncompressed_data_size = data_size;
        self
    }

    pub fn with_intra_block_offset(mut self, offset: u16) -> Self {
        self.intra_block_offset = offset;
        self
    }

    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_optional_fields(mut self, fields: &AccountMetaOptionalFields) -> Self {
        fields.update_flags(&mut self.flags);

        self
    }

    pub fn get_raw_block_size(metas: &Vec<ColdAccountMeta>, index: usize) -> usize {
        let mut block_size = 0;

        for i in index..metas.len() {
            if metas[i].block_offset == metas[index].block_offset {
                block_size += metas[i].uncompressed_data_size;
            } else {
                break;
            }
        }

        block_size.try_into().unwrap()
    }

    pub fn stored_size(
        footer: &TieredStorageFooter,
        metas: &Vec<ColdAccountMeta>,
        i: usize,
    ) -> usize {
        let compressed_block_size = Self::get_compressed_block_size(footer, metas, i);

        let data_size = if metas[i].is_blob_account() {
            compressed_block_size
        } else {
            let compression_rate: f64 =
                compressed_block_size as f64 / Self::get_raw_block_size(metas, i) as f64;

            ((metas[i].uncompressed_data_size as usize + metas[i].optional_fields_size()) as f64
                / compression_rate) as usize
        };

        return std::mem::size_of::<ColdAccountMeta>() + data_size;
    }

    fn get_compressed_block_size(
        footer: &TieredStorageFooter,
        metas: &Vec<ColdAccountMeta>,
        index: usize,
    ) -> usize {
        // Init as if the it is the last data block
        let mut block_size = footer.account_metas_offset - metas[index].block_offset;

        for i in index..metas.len() {
            if metas[i].block_offset == metas[index].block_offset {
                continue;
            }
            block_size = metas[i].block_offset - metas[index].block_offset;
            break;
        }

        block_size.try_into().unwrap()
    }
}

impl Default for ColdAccountMeta {
    fn default() -> Self {
        Self {
            lamports: 0,
            block_offset: 0,
            owner_local_id: 0,
            uncompressed_data_size: 0,
            intra_block_offset: 0,
            flags: AccountMetaFlags::new().to_value(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use {
        crate::{
            account_storage::meta::StoredMetaWriteVersion,
            append_vec::test_utils::get_append_vec_path,
            tiered_storage::{
                cold::ColdAccountMeta,
                file::TieredStorageFile,
                meta_entries::{AccountMetaFlags, AccountMetaOptionalFields, TieredAccountMeta},
            },
        },
        ::solana_sdk::{hash::Hash, stake_history::Epoch},
        memoffset::offset_of,
    };

    #[test]
    fn test_account_meta_entry() {
        let path = get_append_vec_path("test_account_meta_entry");

        const TEST_LAMPORT: u64 = 7;
        const BLOCK_OFFSET: u64 = 56987;
        const OWNER_LOCAL_ID: u32 = 54;
        const UNCOMPRESSED_LENGTH: u16 = 0;
        const LOCAL_OFFSET: u16 = 82;
        const TEST_RENT_EPOCH: Epoch = 7;
        const TEST_WRITE_VERSION: StoredMetaWriteVersion = 0;

        let optional_fields = AccountMetaOptionalFields {
            rent_epoch: Some(TEST_RENT_EPOCH),
            account_hash: Some(Hash::new_unique()),
            write_version_obsolete: Some(TEST_WRITE_VERSION),
        };

        let expected_entry = ColdAccountMeta::new()
            .with_lamports(TEST_LAMPORT)
            .with_block_offset(BLOCK_OFFSET)
            .with_owner_local_id(OWNER_LOCAL_ID)
            .with_uncompressed_data_size(UNCOMPRESSED_LENGTH)
            .with_intra_block_offset(LOCAL_OFFSET)
            .with_flags(
                AccountMetaFlags::new()
                    .with_bit(AccountMetaFlags::EXECUTABLE, true)
                    .to_value(),
            )
            .with_optional_fields(&optional_fields);

        {
            let mut ads_file = TieredStorageFile::new(&path.path, true);
            expected_entry
                .write_account_meta_entry(&mut ads_file)
                .unwrap();
        }

        let mut ads_file = TieredStorageFile::new(&path.path, true);
        let entry = ColdAccountMeta::new_from_file(&mut ads_file).unwrap();

        assert_eq!(expected_entry, entry);
        assert_eq!(entry.flags_get(AccountMetaFlags::EXECUTABLE), true);
        assert_eq!(entry.flags_get(AccountMetaFlags::HAS_RENT_EPOCH), true);
    }

    #[test]
    fn test_cold_account_meta_layout() {
        assert_eq!(offset_of!(ColdAccountMeta, lamports), 0x00);
        assert_eq!(offset_of!(ColdAccountMeta, block_offset), 0x08);
        assert_eq!(offset_of!(ColdAccountMeta, uncompressed_data_size), 0x10);
        assert_eq!(offset_of!(ColdAccountMeta, intra_block_offset), 0x12);
        assert_eq!(offset_of!(ColdAccountMeta, owner_local_id), 0x14);
        assert_eq!(offset_of!(ColdAccountMeta, flags), 0x18);
    }
}
