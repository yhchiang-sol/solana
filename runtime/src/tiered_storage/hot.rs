#![allow(unused_imports)]
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
    std::{collections::HashMap, mem::size_of, option::Option, path::Path},
};

const BLOCK_OFFSET_MASK: u64 = 0x00ff_ffff_ffff_ffff;
const CLEAR_BLOCK_OFFSET_MASK: u64 = 0xff00_0000_0000_0000;
const PADDINGS_MASK: u64 = 0x0700_0000_0000_0000;
const CLEAR_PADDINGS_MASK: u64 = 0xf8ff_ffff_ffff_ffff;
const PADDINGS_SHIFT: u64 = 56;


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
struct HotAccountMeta {
    lamports: u64,
    // the high 8-bits are used to store padding and data block
    // format information.
    // Use block_offset() to obtain the actual block offset.
    block_offset_info: u64,
    owner_index: u32,
    flags: u32,
}

#[allow(dead_code)]
impl HotAccountMeta {
    pub fn new() -> Self {
        HotAccountMeta {
            lamports: 0,
            block_offset_info: 0,
            owner_index: 0,
            flags: 0,
        }
    }
    pub fn with_lamports(mut self, lamports: u64) -> Self {
        self.lamports = lamports;
        self
    }

    pub fn with_owner_index(mut self, owner_index: u32) -> Self {
        self.owner_index = owner_index;
        self
    }

    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }
}

impl TieredAccountMeta for HotAccountMeta {
    fn lamports(&self) -> u64 {
        self.lamports
    }

    fn block_offset(&self) -> u64 {
        (self.block_offset_info & BLOCK_OFFSET_MASK).saturating_mul(8)
    }
    
    fn padding_bytes(&self) -> u8 {
        ((self.block_offset_info & PADDINGS_MASK) >> PADDINGS_SHIFT).try_into().unwrap()
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
        todo!()
    }

    fn intra_block_offset(&self) -> u16 {
        0
    }

    fn owner_local_id(&self) -> u32 {
        self.owner_index
    }

    fn flags_get(&self, _bit_field: u32) -> bool {
        todo!();
    }

    fn rent_epoch(&self, _data_block: &[u8]) -> Option<Epoch> {
        todo!();
    }

    fn account_hash<'a>(&self, _data_block: &'a [u8]) -> &'a Hash {
        todo!();
    }

    fn write_version(&self, _data_block: &[u8]) -> Option<StoredMetaWriteVersion> {
        todo!();
    }

    /*
    fn data_length(&self, data_block: &[u8]) -> Option<u64> {
        Ok(self.optional_fields_offset(data_block) as u64 - self.padding_bytes() as u64)
    }*/

    fn optional_fields_size(&self) -> usize {
        todo!();
    }

    fn optional_fields_offset<'a>(&self, _data_block: &'a [u8]) -> usize {
        todo!();
    }

    fn account_data<'a>(&self, _data_block: &'a [u8]) -> &'a [u8] {
        todo!();
    }

    fn is_blob_account(&self) -> bool {
        todo!();
    }

    fn write_account_meta_entry(&self, _ads_file: &TieredStorageFile) -> std::io::Result<usize> {
        todo!();
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
