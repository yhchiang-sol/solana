use {
    crate::tiered_storage::{file::TieredStorageFile, mmap_utils::get_type},
    memmap2::Mmap,
    serde::{Deserialize, Serialize},
    solana_sdk::{hash::Hash, pubkey::Pubkey},
    std::{mem, path::Path},
};

pub const FOOTER_FORMAT_VERSION: u64 = 1;

static_assertions::const_assert_eq!(mem::size_of::<TieredStorageFooter>(), 184);
// The size of the footer struct + the u64 magic number at the end.
pub const FOOTER_SIZE: i64 = (mem::size_of::<TieredStorageFooter>() + mem::size_of::<u64>()) as i64;
// The size of the ending part of the footer.  This size should remain unchanged
// even when the footer's format changes.
pub const FOOTER_TAIL_SIZE: i64 = 24;

// The ending 8 bytes of a valid tiered account storage file.
pub const FOOTER_MAGIC_NUMBER: u64 = 0x502A2AB5; // SOLALABS -> SOLANA LABS

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct TieredStorageMagicNumber(pub u64);

impl Default for TieredStorageMagicNumber {
    fn default() -> Self {
        Self(FOOTER_MAGIC_NUMBER)
    }
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
pub enum AccountMetaFormat {
    #[default]
    Hot = 0,
    Cold = 1,
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
pub enum AccountDataBlockFormat {
    #[default]
    AlignedRaw = 0,
    Lz4 = 1,
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
pub enum OwnersBlockFormat {
    #[default]
    LocalIndex = 0,
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
pub enum AccountIndexFormat {
    // This format does not support any fast lookup.
    // Any query from account hash to account meta requires linear search.
    #[default]
    Linear = 0,
    // Similar to index, but this format also stores the offset of each account
    // meta in the index block.
    LinearIndex = 1,
}

#[derive(Debug)]
pub struct TieredFileFormat {
    pub meta_entry_size: usize,
    pub account_meta_format: AccountMetaFormat,
    pub owners_block_format: OwnersBlockFormat,
    pub account_index_format: AccountIndexFormat,
    pub data_block_format: AccountDataBlockFormat,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[repr(C)]
pub struct TieredStorageFooter {
    // formats
    pub account_meta_format: AccountMetaFormat,
    pub owners_block_format: OwnersBlockFormat,
    pub account_index_format: AccountIndexFormat,
    pub data_block_format: AccountDataBlockFormat,

    // account-related
    pub account_entry_count: u32,
    pub account_meta_entry_size: u32,
    pub account_data_block_size: u64,

    // owner-related
    pub owner_count: u32,
    pub owner_entry_size: u32,

    // offsets
    // The offset of account meta+data blocks is omitted as it's always 0.
    pub account_pubkeys_offset: u64,
    pub owners_offset: u64,

    // account range
    pub min_account_address: Pubkey,
    pub max_account_address: Pubkey,

    // a hash that represents the tiered account file for consistency check.
    pub hash: Hash,

    // The below fields belong to footer tail.
    // The sum of their sizes should match FOOTER_TAIL_SIZE.
    pub footer_size: u64,
    pub format_version: u64,
    // This field is persisted in the storage but not in this struct.
    // The number should match FOOTER_MAGIC_NUMBER.
    // pub magic_number: u64,
}

impl Default for TieredStorageFooter {
    fn default() -> Self {
        Self {
            account_meta_format: AccountMetaFormat::default(),
            owners_block_format: OwnersBlockFormat::default(),
            account_index_format: AccountIndexFormat::default(),
            data_block_format: AccountDataBlockFormat::default(),
            account_entry_count: 0,
            account_meta_entry_size: 0,
            account_data_block_size: 0,
            owner_count: 0,
            owner_entry_size: 0,
            account_pubkeys_offset: 0,
            owners_offset: 0,
            hash: Hash::new_unique(),
            min_account_address: Pubkey::default(),
            max_account_address: Pubkey::default(),
            footer_size: FOOTER_SIZE as u64,
            format_version: FOOTER_FORMAT_VERSION,
        }
    }
}

impl TieredStorageFooter {
    pub fn new_from_path(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let storage = TieredStorageFile::new_readonly(path);
        Self::new_from_footer_block(&storage)
    }

    pub fn write_footer_block(&self, file: &TieredStorageFile) -> std::io::Result<()> {
        file.write_type(self)?;
        file.write_type(&TieredStorageMagicNumber::default())?;

        Ok(())
    }

    pub fn new_from_footer_block(file: &TieredStorageFile) -> std::io::Result<Self> {
        let mut footer_size: u64 = 0;
        let mut footer_version: u64 = 0;
        let mut magic_number = TieredStorageMagicNumber(0);

        file.seek_from_end(-FOOTER_TAIL_SIZE)?;
        file.read_type(&mut footer_size)?;
        file.read_type(&mut footer_version)?;
        file.read_type(&mut magic_number)?;

        if magic_number != TieredStorageMagicNumber::default() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "TieredStorageError: Magic mumber mismatch",
            ));
        }

        let mut footer = Self::default();
        file.seek_from_end(-(footer_size as i64))?;
        file.read_type(&mut footer)?;

        Ok(footer)
    }

    pub fn new_from_mmap(map: &Mmap) -> std::io::Result<&TieredStorageFooter> {
        let offset = map.len().saturating_sub(FOOTER_TAIL_SIZE as usize);
        let (footer_size, offset) = get_type::<u64>(map, offset)?;
        let (_footer_version, offset) = get_type::<u64>(map, offset)?;
        let (magic_number, _offset) = get_type::<TieredStorageMagicNumber>(map, offset)?;

        if *magic_number != TieredStorageMagicNumber::default() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "TieredStorageError: Magic mumber mismatch",
            ));
        }

        let (footer, _offset): (&TieredStorageFooter, _) =
            get_type(map, map.len().saturating_sub(*footer_size as usize))?;

        Ok(footer)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            append_vec::test_utils::get_append_vec_path, tiered_storage::file::TieredStorageFile,
        },
        memoffset::offset_of,
        solana_sdk::hash::Hash,
        std::mem,
    };

    #[test]
    /// Make sure the in-memory size is what we expected.
    fn test_footer_size() {
        assert_eq!(
            mem::size_of::<TieredStorageFooter>() + mem::size_of::<u64>(),
            FOOTER_SIZE as usize
        );
    }

    #[test]
    fn test_footer() {
        let path = get_append_vec_path("test_file_footer");
        let expected_footer = TieredStorageFooter {
            account_meta_format: AccountMetaFormat::Hot,
            owners_block_format: OwnersBlockFormat::LocalIndex,
            account_index_format: AccountIndexFormat::Linear,
            data_block_format: AccountDataBlockFormat::AlignedRaw,
            account_entry_count: 300,
            account_meta_entry_size: 24,
            account_data_block_size: 4096,
            owner_count: 250,
            owner_entry_size: 32,
            account_pubkeys_offset: 1069600,
            owners_offset: 1081200,
            hash: Hash::new_unique(),
            min_account_address: Pubkey::default(),
            max_account_address: Pubkey::new_unique(),
            footer_size: FOOTER_SIZE as u64,
            format_version: FOOTER_FORMAT_VERSION,
        };

        // Persist the expected footer.
        {
            let file = TieredStorageFile::new_writable(&path.path);
            expected_footer.write_footer_block(&file).unwrap();
        }

        // Reopen the same storage, and expect the persisted footer is
        // the same as what we have written.
        {
            let footer = TieredStorageFooter::new_from_path(&path.path).unwrap();
            assert_eq!(expected_footer, footer);
        }
    }

    #[test]
    fn test_footer_layout() {
        assert_eq!(offset_of!(TieredStorageFooter, account_meta_format), 0x00);
        assert_eq!(offset_of!(TieredStorageFooter, owners_block_format), 0x08);
        assert_eq!(offset_of!(TieredStorageFooter, account_index_format), 0x10);
        assert_eq!(offset_of!(TieredStorageFooter, data_block_format), 0x18);
        assert_eq!(offset_of!(TieredStorageFooter, account_entry_count), 0x20);
        assert_eq!(
            offset_of!(TieredStorageFooter, account_meta_entry_size),
            0x24
        );
        assert_eq!(
            offset_of!(TieredStorageFooter, account_data_block_size),
            0x28
        );
        assert_eq!(offset_of!(TieredStorageFooter, owner_count), 0x30);
        assert_eq!(offset_of!(TieredStorageFooter, owner_entry_size), 0x34);
        assert_eq!(
            offset_of!(TieredStorageFooter, account_pubkeys_offset),
            0x38
        );
        assert_eq!(offset_of!(TieredStorageFooter, owners_offset), 0x40);
        assert_eq!(offset_of!(TieredStorageFooter, min_account_address), 0x48);
        assert_eq!(offset_of!(TieredStorageFooter, max_account_address), 0x68);
        assert_eq!(offset_of!(TieredStorageFooter, hash), 0x88);
        assert_eq!(offset_of!(TieredStorageFooter, footer_size), 0xA8);
        assert_eq!(offset_of!(TieredStorageFooter, format_version), 0xB0);
    }
}
