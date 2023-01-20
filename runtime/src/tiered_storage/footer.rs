use {
    crate::tiered_storage::file::TieredStorageFile,
    serde::{Deserialize, Serialize},
    solana_sdk::hash::Hash,
    std::mem,
};

// The size of the footer struct + the u64 magic number at the end.
pub(crate) const FOOTER_SIZE: i64 =
    (mem::size_of::<TieredStorageFooter>() + mem::size_of::<u64>()) as i64;
pub(crate) const FOOTER_TAIL_SIZE: i64 = 24;

pub(crate) const FOOTER_MAGIC_NUMBER: u64 = 0x501A2AB5; // SOLALABS -> SOLANA LABS

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct TieredStorageMagicNumber {
    pub magic: u64,
}

impl TieredStorageMagicNumber {
    pub fn new() -> Self {
        Self { magic: 0 }
    }
    fn default() -> Self {
        Self {
            magic: FOOTER_MAGIC_NUMBER,
        }
    }
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
#[serde(into = "u64", try_from = "u64")]
pub enum AccountMetaFormat {
    Cold = 0u64,
    Hot = 1u64,
}

impl Default for AccountMetaFormat {
    fn default() -> Self {
        AccountMetaFormat::Cold
    }
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
#[serde(into = "u64", try_from = "u64")]
pub enum AccountDataBlockFormat {
    AlignedRaw = 0u64,
    Lz4 = 1u64,
}

impl Default for AccountDataBlockFormat {
    fn default() -> Self {
        AccountDataBlockFormat::Lz4
    }
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
#[serde(into = "u64", try_from = "u64")]
pub enum OwnersBlockFormat {
    LocalIndex = 0u64,
}

impl Default for OwnersBlockFormat {
    fn default() -> Self {
        OwnersBlockFormat::LocalIndex
    }
}

#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    PartialEq,
    Deserialize,
    num_enum::IntoPrimitive,
    Serialize,
    num_enum::TryFromPrimitive,
)]
#[serde(into = "u64", try_from = "u64")]
pub enum AccountIndexFormat {
    // This format does not support any fast lookup.
    // Any query from account hash to account meta requires linear search.
    Linear = 0u64,
}

impl Default for AccountIndexFormat {
    fn default() -> Self {
        AccountIndexFormat::Linear
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct TieredStorageFooter {
    // formats
    pub account_meta_format: AccountMetaFormat,
    pub owners_block_format: OwnersBlockFormat,
    pub account_index_format: AccountIndexFormat,
    pub data_block_format: AccountDataBlockFormat,

    // regular accounts' stats
    pub account_meta_count: u32,
    pub account_meta_entry_size: u32,
    pub account_data_block_size: u64,

    // owner's stats
    pub owner_count: u32,
    pub owner_entry_size: u32,

    // offsets
    pub account_metas_offset: u64,
    pub account_pubkeys_offset: u64,
    pub owners_offset: u64,

    // misc
    pub hash: Hash,

    // account range
    pub min_account_address: Hash,
    pub max_account_address: Hash,

    // tailing information
    pub footer_size: u64,
    pub format_version: u64,
    // This field is persisted in the storage but not in this struct.
    // pub magic_number: u64,  // FOOTER_MAGIC_NUMBER
}

impl TieredStorageFooter {
    pub fn new() -> Self {
        Self { ..Self::default() }
    }
}

impl Default for TieredStorageFooter {
    fn default() -> Self {
        Self {
            account_meta_format: AccountMetaFormat::default(),
            owners_block_format: OwnersBlockFormat::default(),
            account_index_format: AccountIndexFormat::default(),
            data_block_format: AccountDataBlockFormat::default(),
            account_meta_count: 0,
            account_meta_entry_size: 0,
            account_data_block_size: 0,
            owner_count: 0,
            owner_entry_size: 0,
            account_metas_offset: 0,
            account_pubkeys_offset: 0,
            owners_offset: 0,
            hash: Hash::new_unique(),
            min_account_address: Hash::default(),
            max_account_address: Hash::default(),
            footer_size: FOOTER_SIZE as u64,
            format_version: 1,
        }
    }
}

impl TieredStorageFooter {
    pub fn write_footer_block(&self, ads_file: &TieredStorageFile) -> std::io::Result<()> {
        ads_file.write_type(self)?;
        ads_file.write_type(&TieredStorageMagicNumber::default())?;

        Ok(())
    }

    pub fn new_from_footer_block(ads_file: &TieredStorageFile) -> std::io::Result<Self> {
        let mut footer_size: u64 = 0;
        let mut footer_version: u64 = 0;
        let mut magic_number = TieredStorageMagicNumber::new();

        ads_file.seek_from_end(-FOOTER_TAIL_SIZE)?;
        ads_file.read_type(&mut footer_size)?;
        ads_file.read_type(&mut footer_version)?;
        ads_file.read_type(&mut magic_number)?;

        if magic_number != TieredStorageMagicNumber::default() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "TieredStorageError: Magic mumber mismatch",
            ));
        }

        let mut footer = Self::new();
        ads_file.seek_from_end(-(footer_size as i64))?;
        ads_file.read_type(&mut footer)?;

        Ok(footer)
    }
}

#[cfg(test)]
pub mod tests {
    use {
        crate::{
            append_vec::test_utils::get_append_vec_path,
            tiered_storage::{
                file::TieredStorageFile,
                footer::{
                    AccountDataBlockFormat, AccountIndexFormat, AccountMetaFormat,
                    OwnersBlockFormat, TieredStorageFooter, FOOTER_SIZE,
                },
            },
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
            account_meta_count: 300,
            account_meta_entry_size: 24,
            account_data_block_size: 4096,
            owner_count: 250,
            owner_entry_size: 32,
            account_metas_offset: 1062400,
            account_pubkeys_offset: 1069600,
            owners_offset: 1081200,
            hash: Hash::new_unique(),
            min_account_address: Hash::default(),
            max_account_address: Hash::default(),
            footer_size: FOOTER_SIZE as u64,
            format_version: 1,
        };

        {
            let ads_file = TieredStorageFile::new(&path.path, true);
            expected_footer.write_footer_block(&ads_file).unwrap();
        }

        // Reopen the same storage, and expect the persisted footer is
        // the same as what we have written.
        {
            let ads_file = TieredStorageFile::new(&path.path, true);
            let footer = TieredStorageFooter::new_from_footer_block(&ads_file).unwrap();
            assert_eq!(expected_footer, footer);
        }
    }

    #[test]
    fn test_footer_layout() {
        assert_eq!(offset_of!(TieredStorageFooter, account_meta_format), 0x08);
        assert_eq!(offset_of!(TieredStorageFooter, owners_block_format), 0x10);
        assert_eq!(offset_of!(TieredStorageFooter, account_index_format), 0x18);
        assert_eq!(offset_of!(TieredStorageFooter, data_block_format), 0x20);
        assert_eq!(offset_of!(TieredStorageFooter, account_meta_count), 0x28);
        assert_eq!(
            offset_of!(TieredStorageFooter, account_meta_entry_size),
            0x2C
        );
        assert_eq!(
            offset_of!(TieredStorageFooter, account_data_block_size),
            0x30
        );
        assert_eq!(offset_of!(TieredStorageFooter, owner_count), 0x38);
        assert_eq!(offset_of!(TieredStorageFooter, owner_entry_size), 0x3C);
        assert_eq!(offset_of!(TieredStorageFooter, account_metas_offset), 0x40);
        assert_eq!(
            offset_of!(TieredStorageFooter, account_pubkeys_offset),
            0x48
        );
        assert_eq!(offset_of!(TieredStorageFooter, owners_offset), 0x50);
        assert_eq!(offset_of!(TieredStorageFooter, data_block_format), 0x58);
        assert_eq!(offset_of!(TieredStorageFooter, hash), 0x60);
        assert_eq!(offset_of!(TieredStorageFooter, min_account_address), 0x80);
        assert_eq!(offset_of!(TieredStorageFooter, max_account_address), 0xA0);
        assert_eq!(offset_of!(TieredStorageFooter, footer_size), 0xC0);
        assert_eq!(offset_of!(TieredStorageFooter, format_version), 0xC8);
    }
}
