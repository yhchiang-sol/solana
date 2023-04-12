#![allow(dead_code)]
//! The account meta and related structs for the tiered storage.
use {
    crate::{
        account_storage::meta::StoredMetaWriteVersion,
        tiered_storage::{
            file::TieredStorageFile, footer::TieredStorageFooter, TieredStorageResult,
        },
    },
    ::solana_sdk::{hash::Hash, stake_history::Epoch},
    modular_bitfield::prelude::*,
    std::mem::size_of,
};

/// The struct that handles the account meta flags.
#[allow(dead_code)]
#[bitfield(bits = 32)]
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub struct AccountMetaFlags {
    /// whether the account meta has rent epoch
    pub has_rent_epoch: bool,
    /// whether the account meta has account hash
    pub has_account_hash: bool,
    /// whether the account meta has write version
    pub has_write_version: bool,
    /// is the account data is executable
    pub executable: bool,
    /// the reserved bits.
    reserved: B28,
}

lazy_static! {
    pub static ref DEFAULT_ACCOUNT_HASH: Hash = Hash::default();
}

pub const ACCOUNT_DATA_ENTIRE_BLOCK: u16 = std::u16::MAX;

// TODO(yhchiang): this function needs to be fixed.
pub(crate) fn get_compressed_block_size(
    _footer: &TieredStorageFooter,
    _metas: &Vec<impl TieredAccountMeta>,
    _index: usize,
) -> usize {
    unimplemented!();
}

pub trait TieredAccountMeta {
    fn new() -> Self;

    fn is_blob_account_data(data_len: u64) -> bool;

    fn with_lamports(&mut self, _lamports: u64) -> &mut Self {
        unimplemented!();
    }

    fn with_block_offset(&mut self, _offset: u64) -> &mut Self {
        unimplemented!();
    }

    fn with_data_tailing_paddings(&mut self, _paddings: u8) -> &mut Self {
        unimplemented!();
    }

    fn with_owner_local_id(&mut self, _local_id: u32) -> &mut Self {
        unimplemented!();
    }

    fn with_uncompressed_data_size(&mut self, _data_size: u64) -> &mut Self {
        unimplemented!();
    }

    fn with_intra_block_offset(&mut self, _offset: u16) -> &mut Self {
        unimplemented!();
    }

    fn with_flags(&mut self, _flags: &AccountMetaFlags) -> &mut Self {
        unimplemented!();
    }

    fn lamports(&self) -> u64;
    fn block_offset(&self) -> u64;
    fn set_block_offset(&mut self, offset: u64);
    fn padding_bytes(&self) -> u8;
    fn uncompressed_data_size(&self) -> usize {
        unimplemented!();
    }
    fn intra_block_offset(&self) -> u16;
    fn owner_local_id(&self) -> u32;
    fn flags(&self) -> &AccountMetaFlags;
    fn rent_epoch(&self, data_block: &[u8]) -> Option<Epoch>;
    fn account_hash<'a>(&self, data_block: &'a [u8]) -> &'a Hash;
    fn write_version(&self, data_block: &[u8]) -> Option<StoredMetaWriteVersion>;
    fn optional_fields_size(&self) -> usize {
        let mut size_in_bytes = 0;
        if self.flags().has_rent_epoch() {
            size_in_bytes += size_of::<Epoch>();
        }
        if self.flags().has_account_hash() {
            size_in_bytes += size_of::<Hash>();
        }
        if self.flags().has_write_version() {
            size_in_bytes += size_of::<StoredMetaWriteVersion>();
        }

        size_in_bytes
    }

    fn optional_fields_offset<'a>(&self, data_block: &'a [u8]) -> usize;
    fn data_len(&self, data_block: &[u8]) -> usize;
    fn account_data<'a>(&self, data_block: &'a [u8]) -> &'a [u8];
    fn is_blob_account(&self) -> bool;
    fn write_account_meta_entry(&self, ads_file: &TieredStorageFile) -> TieredStorageResult<usize>;
    fn stored_size(
        footer: &TieredStorageFooter,
        metas: &Vec<impl TieredAccountMeta>,
        i: usize,
    ) -> usize;
}

impl AccountMetaFlags {
    fn new_from(optional_fields: &AccountMetaOptionalFields) -> Self {
        let mut flags = AccountMetaFlags::default();
        flags.set_has_rent_epoch(optional_fields.rent_epoch.is_some());
        flags.set_has_account_hash(optional_fields.account_hash.is_some());
        flags.set_has_write_version(optional_fields.write_version.is_some());
        flags
    }
}

/// The in-memory struct for the optional fields for tiered account meta.
///
/// Note that the storage representation of the optional fields might be
/// different from its in-memory representation.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AccountMetaOptionalFields {
    /// the epoch at which its associated account will next owe rent
    pub rent_epoch: Option<Epoch>,
    /// the hash of its associated account
    pub account_hash: Option<Hash>,
    /// Order of stores of its associated account to an accounts file will
    /// determine 'latest' account data per pubkey.
    pub write_version: Option<StoredMetaWriteVersion>,
}

impl AccountMetaOptionalFields {
    /// The size of the optional fields in bytes (excluding the boolean flags).
    pub fn size(&self) -> usize {
        self.rent_epoch.map_or(0, |_| std::mem::size_of::<Epoch>())
            + self.account_hash.map_or(0, |_| std::mem::size_of::<Hash>())
            + self
                .write_version
                .map_or(0, |_| std::mem::size_of::<StoredMetaWriteVersion>())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_account_meta_flags_new() {
        let flags = AccountMetaFlags::new();

        assert!(!flags.has_rent_epoch());
        assert!(!flags.has_account_hash());
        assert!(!flags.has_write_version());
        assert_eq!(flags.reserved(), 0u32);

        assert_eq!(
            std::mem::size_of::<AccountMetaFlags>(),
            std::mem::size_of::<u32>()
        );
    }

    fn verify_flags_serialization(flags: &AccountMetaFlags) {
        assert_eq!(AccountMetaFlags::from_bytes(flags.into_bytes()), *flags);
    }

    #[test]
    fn test_account_meta_flags_set() {
        let mut flags = AccountMetaFlags::new();

        flags.set_has_rent_epoch(true);

        assert!(flags.has_rent_epoch());
        assert!(!flags.has_account_hash());
        assert!(!flags.has_write_version());
        verify_flags_serialization(&flags);

        flags.set_has_account_hash(true);

        assert!(flags.has_rent_epoch());
        assert!(flags.has_account_hash());
        assert!(!flags.has_write_version());
        verify_flags_serialization(&flags);

        flags.set_has_write_version(true);

        assert!(flags.has_rent_epoch());
        assert!(flags.has_account_hash());
        assert!(flags.has_write_version());
        verify_flags_serialization(&flags);

        // make sure the reserved bits are untouched.
        assert_eq!(flags.reserved(), 0u32);
    }

    fn update_and_verify_flags(opt_fields: &AccountMetaOptionalFields) {
        let flags: AccountMetaFlags = AccountMetaFlags::new_from(opt_fields);
        assert_eq!(flags.has_rent_epoch(), opt_fields.rent_epoch.is_some());
        assert_eq!(flags.has_account_hash(), opt_fields.account_hash.is_some());
        assert_eq!(
            flags.has_write_version(),
            opt_fields.write_version.is_some()
        );
        assert_eq!(flags.reserved(), 0u32);
    }

    #[test]
    fn test_optional_fields_update_flags() {
        let test_epoch = 5432312;
        let test_write_version = 231;

        for rent_epoch in [None, Some(test_epoch)] {
            for account_hash in [None, Some(Hash::new_unique())] {
                for write_version in [None, Some(test_write_version)] {
                    update_and_verify_flags(&AccountMetaOptionalFields {
                        rent_epoch,
                        account_hash,
                        write_version,
                    });
                }
            }
        }
    }

    #[test]
    fn test_optional_fields_size() {
        let test_epoch = 5432312;
        let test_write_version = 231;

        for rent_epoch in [None, Some(test_epoch)] {
            for account_hash in [None, Some(Hash::new_unique())] {
                for write_version in [None, Some(test_write_version)] {
                    let opt_fields = AccountMetaOptionalFields {
                        rent_epoch,
                        account_hash,
                        write_version,
                    };
                    assert_eq!(
                        opt_fields.size(),
                        rent_epoch.map_or(0, |_| std::mem::size_of::<Epoch>())
                            + account_hash.map_or(0, |_| std::mem::size_of::<Hash>())
                            + write_version
                                .map_or(0, |_| std::mem::size_of::<StoredMetaWriteVersion>())
                    );
                }
            }
        }
    }
}
