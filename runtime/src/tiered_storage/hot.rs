#![allow(unused_imports)]
use {
    crate::{
        account_storage::meta::{StoredAccountMeta, StoredMetaWriteVersion},
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        append_vec::MatchAccountOwnerError,
        tiered_storage::{
            data_block::AccountDataBlock,
            file::TieredStorageFile,
            footer::{AccountDataBlockFormat, TieredStorageFooter},
            meta_entries::{
                get_compressed_block_size,
                AccountMetaFlags, AccountMetaOptionalFields, TieredAccountMeta,
                ACCOUNT_DATA_ENTIRE_BLOCK, DEFAULT_ACCOUNT_HASH,
            },
            reader::{TieredStorageReader, TieredStoredAccountMeta},
        },
    },
    solana_sdk::{hash::Hash, pubkey::Pubkey, stake_history::Epoch},
    std::{collections::HashMap, mem::size_of, option::Option, path::Path},
};

const BLOCK_OFFSET_MASK: u64 = 0x00ff_ffff_ffff_ffff;
const CLEAR_BLOCK_OFFSET_MASK: u64 = 0xff00_0000_0000_0000;
const PADDINGS_MASK: u64 = 0x0700_0000_0000_0000;
const CLEAR_PADDINGS_MASK: u64 = 0xf8ff_ffff_ffff_ffff;
const PADDINGS_SHIFT: u64 = 56;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct HotAccountMeta {
    lamports: u64,
    // the high 8-bits are used to store padding and data block
    // format information.
    // Use block_offset() to obtain the actual block offset.
    block_offset_info: u64,
    owner_index: u32,
    flags: u32,
}

impl TieredAccountMeta for HotAccountMeta {
    fn new() -> Self {
        HotAccountMeta {
            lamports: 0,
            block_offset_info: 0,
            owner_index: 0,
            flags: 0,
        }
    }

    fn lamports(&self) -> u64 {
        self.lamports
    }

    fn with_lamports(&mut self, lamports: u64) -> &mut Self {
        self.lamports = lamports;
        self
    }

    fn with_owner_local_id(&mut self, owner_index: u32) -> &mut Self {
        self.owner_index = owner_index;
        self
    }

    fn with_uncompressed_data_size(&mut self, _data_size: u16) -> &mut Self {
        // hot meta always have its own data block unless its
        // block_offset_info indicates it is inside a shared block
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
        (self.block_offset_info & BLOCK_OFFSET_MASK).saturating_mul(8)
    }

    fn padding_bytes(&self) -> u8 {
        ((self.block_offset_info & PADDINGS_MASK) >> PADDINGS_SHIFT)
            .try_into()
            .unwrap()
    }

    fn set_block_offset(&mut self, offset: u64) {
        assert!((offset >> 3) <= BLOCK_OFFSET_MASK);
        self.block_offset_info &= CLEAR_BLOCK_OFFSET_MASK;
        self.block_offset_info |= offset >> 3;
    }

    fn set_padding_bytes(&mut self, paddings: u8) {
        assert!(paddings <= 7);
        self.block_offset_info &= CLEAR_PADDINGS_MASK;
        self.block_offset_info |= (paddings as u64) << PADDINGS_SHIFT;
    }

    fn uncompressed_data_size(&self) -> u16 {
        // hot meta always have its own data block unless its
        // block_offset_info indicates it is inside a shared block
        ACCOUNT_DATA_ENTIRE_BLOCK
    }

    fn intra_block_offset(&self) -> u16 {
        // hot meta always have intra block offset equals to 0 except
        // its block_offset_info indocates it is inside a shared block.
        0
    }

    fn owner_local_id(&self) -> u32 {
        self.owner_index
    }

    fn flags_get(&self, bit_field: u32) -> bool {
        AccountMetaFlags::get(&self.flags, bit_field)
    }

    fn rent_epoch(&self, data_block: &[u8]) -> Option<Epoch> {
        let _offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            todo!();
        }
        None
    }

    fn account_hash<'a>(&self, data_block: &'a [u8]) -> &'a Hash {
        let mut _offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            _offset += std::mem::size_of::<Epoch>();
        }
        if self.flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH) {
            todo!();
        }
        return &DEFAULT_ACCOUNT_HASH;
    }

    fn write_version(&self, data_block: &[u8]) -> Option<StoredMetaWriteVersion> {
        let mut _offset = self.optional_fields_offset(data_block);
        if self.flags_get(AccountMetaFlags::HAS_RENT_EPOCH) {
            _offset += std::mem::size_of::<Epoch>();
        }
        if self.flags_get(AccountMetaFlags::HAS_ACCOUNT_HASH) {
            _offset += std::mem::size_of::<Hash>();
        }
        if self.flags_get(AccountMetaFlags::HAS_WRITE_VERSION) {
            todo!();
        }
        None
    }

    fn data_len(&self, data_block: &[u8]) -> usize {
        self.optional_fields_offset(data_block).saturating_sub(
            self.padding_bytes() as usize)
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

    fn write_account_meta_entry(&self, _ads_file: &TieredStorageFile) -> std::io::Result<usize> {
        todo!();
    }

    fn stored_size(
        footer: &TieredStorageFooter,
        metas: &Vec<impl TieredAccountMeta>,
        i: usize,
    ) -> usize {
        // hot storage does not compress so the returned size is the data size.
        let data_size = get_compressed_block_size(footer, metas, i);

        return std::mem::size_of::<HotAccountMeta>() + data_size;
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
                hot::HotAccountMeta,
                meta_entries::{AccountMetaFlags, AccountMetaOptionalFields, TieredAccountMeta},
            },
        },
        ::solana_sdk::{hash::Hash, stake_history::Epoch},
        memoffset::offset_of,
        std::mem::size_of,
    };

    #[test]
    fn test_hot_account_meta_layout() {
        assert_eq!(offset_of!(HotAccountMeta, lamports), 0x00);
        assert_eq!(offset_of!(HotAccountMeta, block_offset_info), 0x08);
        assert_eq!(offset_of!(HotAccountMeta, owner_index), 0x10);
        assert_eq!(offset_of!(HotAccountMeta, flags), 0x14);
        assert_eq!(std::mem::size_of::<HotAccountMeta>(), 24);
    }

    #[test]
    fn test_hot_offset_and_padding() {
        let mut hot_meta = HotAccountMeta::new();
        let offset: u64 = 0x07ff_ef98_7654_3218;
        let paddings: u8 = 3;
        hot_meta.set_block_offset(offset);
        hot_meta.set_padding_bytes(paddings);
        assert_eq!(hot_meta.block_offset(), offset);
        assert_eq!(hot_meta.padding_bytes(), paddings);
    }

    #[test]
    fn test_max_hot_offset_and_padding() {
        let mut hot_meta = HotAccountMeta::new();
        // hot offset must be a multiple of 8.
        let offset: u64 = 0x07ff_ffff_ffff_fff8;
        let paddings: u8 = 7;
        hot_meta.set_block_offset(offset);
        hot_meta.set_padding_bytes(paddings);
        assert_eq!(hot_meta.block_offset(), offset);
        assert_eq!(hot_meta.padding_bytes(), paddings);
    }
}
