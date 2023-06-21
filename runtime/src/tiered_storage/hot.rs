#![allow(dead_code)]
use {
    crate::tiered_storage::meta::{AccountMetaFlags, TieredAccountMeta},
    modular_bitfield::prelude::*,
};

const MAX_HOT_OWNER_INDEX: u32 = (1 << 29) - 1;
const MAX_HOT_PADDING: u8 = 7;

#[bitfield(bits = 32)]
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub(crate) struct HotMetaPackedFields {
    pub(crate) padding: B3,
    pub(crate) owner_local_id: B29,
}

#[derive(Debug, PartialEq, Eq)]
#[repr(C)]
pub struct HotAccountMeta {
    pub(crate) lamports: u64,
    // the high 8-bits are used to store padding and data block
    // format information.
    // Use block_offset() to obtain the actual block offset.
    // block_offset_info: u64,
    pub(crate) packed_fields: HotMetaPackedFields,
    pub(crate) flags: AccountMetaFlags,
}

impl HotAccountMeta {
    fn set_padding_bytes(&mut self, padding: u8) {
        assert!(padding <= 7);
        self.packed_fields.set_padding(padding);
    }
}

impl TieredAccountMeta for HotAccountMeta {
    fn new() -> Self {
        HotAccountMeta {
            lamports: 0,
            packed_fields: HotMetaPackedFields::default(),
            flags: AccountMetaFlags::new(),
        }
    }

    fn with_lamports(&mut self, lamports: u64) -> &mut Self {
        self.lamports = lamports;
        self
    }

    fn with_account_data_padding(&mut self, paddings: u8) -> &mut Self {
        self.set_padding_bytes(paddings);
        self
    }

    fn with_owner_local_id(&mut self, owner_local_id: u32) -> &mut Self {
        assert!(owner_local_id <= MAX_HOT_OWNER_INDEX);
        self.packed_fields.set_owner_local_id(owner_local_id);
        self
    }

    fn with_uncompressed_data_size(&mut self, _data_size: u64) -> &mut Self {
        // Hot meta does not store its data size as it derives its data length
        // by comparing the offets of two consecutive account meta entries.
        self
    }

    fn with_flags(&mut self, flags: &AccountMetaFlags) -> &mut Self {
        self.flags = *flags;
        self
    }
}

#[cfg(test)]
pub mod tests {
    use {
        super::*,
        crate::tiered_storage::meta::AccountMetaOptionalFields,
        memoffset::offset_of,
        solana_sdk::{hash::Hash, pubkey::Pubkey, stake_history::Epoch},
    };

    #[test]
    fn test_hot_account_meta_layout() {
        assert_eq!(offset_of!(HotAccountMeta, lamports), 0x00);
        assert_eq!(offset_of!(HotAccountMeta, packed_fields), 0x08);
        assert_eq!(offset_of!(HotAccountMeta, flags), 0x0C);
        assert_eq!(std::mem::size_of::<HotAccountMeta>(), 16);
    }

    #[test]
    fn test_packed_fields() {
        const TEST_PADDING: u8 = 7;
        const TEST_OWNER_INDEX: u32 = 0x1fff_ef98;
        let mut packed_fields = HotMetaPackedFields::default();
        packed_fields.set_padding(TEST_PADDING);
        packed_fields.set_owner_local_id(TEST_OWNER_INDEX);
        assert_eq!(packed_fields.padding(), TEST_PADDING);
        assert_eq!(packed_fields.owner_local_id(), TEST_OWNER_INDEX);
    }

    #[test]
    fn test_packed_fields_max_values() {
        let mut packed_fields = HotMetaPackedFields::default();
        packed_fields.set_padding(MAX_HOT_PADDING);
        packed_fields.set_owner_local_id(MAX_HOT_OWNER_INDEX);
        assert_eq!(packed_fields.padding(), MAX_HOT_PADDING);
        assert_eq!(packed_fields.owner_local_id(), MAX_HOT_OWNER_INDEX);
    }

    #[test]
    fn test_hot_account_meta() {
        const TEST_LAMPORTS: u64 = 2314232137;
        const TEST_PADDING: u8 = 5;
        const TEST_OWNER_LOCAL_ID: u32 = 0x1fef_1234;
        const TEST_RENT_EPOCH: Epoch = 7;

        let optional_fields = AccountMetaOptionalFields {
            rent_epoch: Some(TEST_RENT_EPOCH),
            account_hash: Some(Hash::new_unique()),
            write_version: None,
        };

        let mut meta = HotAccountMeta::new();
        let flags = AccountMetaFlags::new_from(&optional_fields);
        meta.with_lamports(TEST_LAMPORTS)
            .with_account_data_padding(TEST_PADDING)
            .with_owner_local_id(TEST_OWNER_LOCAL_ID)
            .with_flags(&flags);

        assert_eq!(meta.flags, flags);
        assert_eq!(meta.packed_fields.owner_local_id(), TEST_OWNER_LOCAL_ID);
        assert_eq!(meta.packed_fields.padding(), TEST_PADDING);
        assert_eq!(meta.lamports, TEST_LAMPORTS);
    }
}
