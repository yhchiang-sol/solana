use {
    crate::{
        account_storage::meta::StoredMetaWriteVersion,
        tiered_storage::{file::TieredStorageFile, AccountDataBlockWriter},
    },
    ::solana_sdk::{hash::Hash, stake_history::Epoch},
    serde::{Deserialize, Serialize},
    std::mem::size_of,
};

pub const ACCOUNT_META_ENTRY_SIZE_BYTES: u32 = 32;
pub const ACCOUNT_DATA_ENTIRE_BLOCK: u16 = std::u16::MAX;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct AccountMetaFlags {
    flags: u32,
}

impl AccountMetaFlags {
    pub const EXECUTABLE: u32 = 1u32;
    pub const HAS_RENT_EPOCH: u32 = 1u32 << 1;
    pub const HAS_ACCOUNT_HASH: u32 = 1u32 << 2;
    pub const HAS_WRITE_VERSION: u32 = 1u32 << 3;
    pub const HAS_DATA_LENGTH: u32 = 1u32 << 4;

    pub fn new() -> Self {
        Self { flags: 0 }
    }

    pub fn new_from(value: u32) -> Self {
        Self { flags: value }
    }

    pub fn with_bit(mut self, bit_field: u32, value: bool) -> Self {
        self.set(bit_field, value);

        self
    }

    pub fn to_value(self) -> u32 {
        self.flags
    }

    pub fn set(&mut self, bit_field: u32, value: bool) {
        if value == true {
            self.flags |= bit_field;
        } else {
            self.flags &= !bit_field;
        }
    }

    pub fn get(flags: &u32, bit_field: u32) -> bool {
        (flags & bit_field) > 0
    }

    pub fn get_value(&self) -> u32 {
        self.flags
    }

    pub fn get_value_mut(&mut self) -> &mut u32 {
        &mut self.flags
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AccountMetaOptionalFields {
    pub rent_epoch: Option<Epoch>,
    pub account_hash: Option<Hash>,
    pub write_version_obsolete: Option<StoredMetaWriteVersion>,
}

impl AccountMetaOptionalFields {
    /// Returns the 16-bit value where each bit represesnts whether one
    /// optional field has a Some value.
    pub fn update_flags(&self, flags_value: &mut u32) {
        let mut flags = AccountMetaFlags::new_from(*flags_value);
        flags.set(AccountMetaFlags::HAS_RENT_EPOCH, self.rent_epoch.is_some());
        flags.set(
            AccountMetaFlags::HAS_ACCOUNT_HASH,
            self.account_hash.is_some(),
        );
        flags.set(
            AccountMetaFlags::HAS_WRITE_VERSION,
            self.write_version_obsolete.is_some(),
        );
        *flags_value = flags.to_value();
    }

    pub fn size(&self) -> usize {
        let mut size_in_bytes = 0;
        if self.rent_epoch.is_some() {
            size_in_bytes += size_of::<Epoch>();
        }
        if self.account_hash.is_some() {
            size_in_bytes += size_of::<Hash>();
        }
        if self.write_version_obsolete.is_some() {
            size_in_bytes += size_of::<StoredMetaWriteVersion>();
        }

        size_in_bytes
    }

    pub fn write(&self, writer: &mut AccountDataBlockWriter) -> std::io::Result<usize> {
        let mut length = 0;
        if let Some(rent_epoch) = self.rent_epoch {
            length += writer.write_type(&rent_epoch)?;
        }
        if let Some(hash) = self.account_hash {
            length += writer.write_type(&hash)?;
        }
        if let Some(write_version) = self.write_version_obsolete {
            length += writer.write_type(&write_version)?;
        }

        Ok(length)
    }
}

pub trait TieredAccountMeta {
    fn lamports(&self) -> u64;
    fn block_offset(&self) -> u64;
    fn set_block_offset(&mut self, offset: u64);
    fn padding_bytes(&self) -> u8;
    fn set_padding_bytes(&mut self, paddings: u8);
    fn uncompressed_data_size(&self) -> u16;
    fn intra_block_offset(&self) -> u16;
    fn owner_local_id(&self) -> u32;
    fn flags_get(&self, bit_field: u32) -> bool;
    fn rent_epoch(&self, data_block: &[u8]) -> Option<Epoch>;
    fn account_hash<'a>(&self, data_block: &'a [u8]) -> &'a Hash;
    fn write_version(&self, data_block: &[u8]) -> Option<StoredMetaWriteVersion>;

    // fn data_length(&self, data_block: &[u8]) -> Option<u64>;
    fn optional_fields_size(&self) -> usize;
    fn optional_fields_offset<'a>(&self, data_block: &'a [u8]) -> usize;
    fn account_data<'a>(&self, data_block: &'a [u8]) -> &'a [u8];
    fn is_blob_account(&self) -> bool;
    fn write_account_meta_entry(&self, ads_file: &TieredStorageFile) -> std::io::Result<usize>;
}

#[cfg(test)]
pub mod tests {
    use crate::tiered_storage::meta_entries::AccountMetaFlags;

    impl AccountMetaFlags {
        pub(crate) fn get_test(&self, bit_field: u32) -> bool {
            (self.flags & bit_field) > 0
        }
    }

    #[test]
    fn test_flags() {
        let mut flags = AccountMetaFlags::new();
        assert_eq!(flags.get_test(AccountMetaFlags::EXECUTABLE), false);
        assert_eq!(flags.get_test(AccountMetaFlags::HAS_RENT_EPOCH), false);

        flags.set(AccountMetaFlags::EXECUTABLE, true);
        assert_eq!(flags.get_test(AccountMetaFlags::EXECUTABLE), true);
        assert_eq!(flags.get_test(AccountMetaFlags::HAS_RENT_EPOCH), false);

        flags.set(AccountMetaFlags::HAS_RENT_EPOCH, true);
        assert_eq!(flags.get_test(AccountMetaFlags::EXECUTABLE), true);
        assert_eq!(flags.get_test(AccountMetaFlags::HAS_RENT_EPOCH), true);

        flags.set(AccountMetaFlags::EXECUTABLE, false);
        assert_eq!(flags.get_test(AccountMetaFlags::EXECUTABLE), false);
        assert_eq!(flags.get_test(AccountMetaFlags::HAS_RENT_EPOCH), true);

        flags.set(AccountMetaFlags::HAS_RENT_EPOCH, false);
        assert_eq!(flags.get_test(AccountMetaFlags::EXECUTABLE), false);
        assert_eq!(flags.get_test(AccountMetaFlags::HAS_RENT_EPOCH), false);
    }
}
