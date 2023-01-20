use {
    crate::tiered_storage::{
        file::TieredStorageFile, footer::TieredStorageFooter, mmap_utils::get_type,
    },
    memmap2::Mmap,
    solana_sdk::pubkey::Pubkey,
};

// This is in-memory struct is only used in the writer.
// The actual storage format of the tiered account index is different.
pub struct AccountIndexWriterEntry {
    pub pubkey: Pubkey,
    pub block_offset: u64,
    pub intra_block_offset: u64,
}

pub struct HotAccountIndexer {}

impl HotAccountIndexer {
    pub fn write_index_block(
        file: &TieredStorageFile,
        index_entries: &Vec<AccountIndexWriterEntry>,
    ) -> std::io::Result<u64> {
        let mut cursor: u64 = 0;
        for index_entry in index_entries {
            cursor += file.write_type(&index_entry.pubkey)? as u64;
        }
        for index_entry in index_entries {
            cursor += file.write_type(&index_entry.block_offset)? as u64;
        }
        Ok(cursor)
    }

    pub fn get_pubkey_offset(footer: &TieredStorageFooter, index: usize) -> usize {
        footer.account_index_offset as usize + std::mem::size_of::<Pubkey>() * index
    }

    pub fn get_meta_offset(
        map: &Mmap,
        footer: &TieredStorageFooter,
        index: usize,
    ) -> std::io::Result<u64> {
        let offset = footer.account_index_offset as usize
            + std::mem::size_of::<Pubkey>() * (footer.account_entry_count as usize)
            + index * std::mem::size_of::<u64>();
        let (meta_offset, _) = get_type(map, offset)?;
        Ok(*meta_offset)
    }

    pub fn entry_size() -> usize {
        std::mem::size_of::<Pubkey>() + std::mem::size_of::<u64>()
    }
}
