use {
    crate::tiered_storage::{
        file::TieredStorageFile,
        footer::TieredStorageFooter,
        mmap_utils::{get_slice, get_type},
        TieredStorageResult,
    },
    memmap2::Mmap,
    solana_sdk::pubkey::Pubkey,
};

/// The in-memory struct for the writing index block.
/// The actual storage format of a tiered account index entry might be different
/// from this.
#[derive(Debug)]
pub struct AccountIndexWriterEntry<'a> {
    pub address: &'a Pubkey,
    pub block_offset: u64,
    pub intra_block_offset: u64,
}

/// The index format of a tiered accounts file.
#[repr(u16)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    PartialEq,
    num_enum::IntoPrimitive,
    num_enum::TryFromPrimitive,
)]
pub enum AccountIndex {
    /// This format optimizes the storage size by storing only account addresses
    /// and offsets.  It skips storing the size of account data by storing account
    /// block entries and index block entries in the same order.
    #[default]
    AddressAndOffset = 0,
}

impl AccountIndex {
    /// Persists the specified index_entries to the specified file and returns
    /// the total number of bytes written.
    pub fn write_index_block(
        &self,
        file: &TieredStorageFile,
        index_entries: &[AccountIndexWriterEntry],
    ) -> TieredStorageResult<usize> {
        match self {
            Self::AddressAndOffset => {
                let mut bytes_written = 0;
                for index_entry in index_entries {
                    bytes_written += file.write_type(index_entry.address)?;
                }
                for index_entry in index_entries {
                    bytes_written += file.write_type(&index_entry.block_offset)?;
                }
                Ok(bytes_written)
            }
        }
    }

    /// Returns the address of the account given the specified index.
    pub fn get_account_address<'a>(
        &self,
        map: &'a Mmap,
        footer: &TieredStorageFooter,
        index: usize,
    ) -> TieredStorageResult<&'a Pubkey> {
        let offset = match self {
            Self::AddressAndOffset => {
                footer.account_index_offset as usize + std::mem::size_of::<Pubkey>() * index
            }
        };
        let (address, _) = get_type::<Pubkey>(map, offset)?;
        Ok(address)
    }

    /// Returns the offset and size of the account block that contains
    /// the account associated with the specified index to the index block.
    fn get_account_block_info(
        &self,
        mmap: &Mmap,
        footer: &TieredStorageFooter,
        index: usize,
    ) -> TieredStorageResult<(u64, usize)> {
        match self {
            Self::AddressAndOffset => {
                let index_offset = footer.account_index_offset as usize
                    + std::mem::size_of::<Pubkey>() * footer.account_entry_count as usize
                    + index * std::mem::size_of::<u64>();
                let (target_block_offset, mut index_offset) = get_type::<u64>(mmap, index_offset)?;
                let owners_block_offset: usize = footer.owners_offset.try_into().unwrap();

                let next_block_offset = loop {
                    if index_offset >= owners_block_offset {
                        break footer.account_index_offset as usize;
                    }
                    let (block_offset, next_offset) = get_type::<u64>(mmap, index_offset)?;
                    if *target_block_offset != *block_offset {
                        break *block_offset as usize;
                    }
                    index_offset = next_offset;
                };
                return Ok((
                    *target_block_offset,
                    next_block_offset - (*target_block_offset) as usize,
                ));
            }
        }
    }

    /// Returns the account block from the specified mmap
    pub fn get_account_block<'a>(
        &self,
        mmap: &'a Mmap,
        footer: &TieredStorageFooter,
        index: usize,
    ) -> TieredStorageResult<&'a [u8]> {
        let (offset, len) = self.get_account_block_info(mmap, footer, index)?;
        let (account_block, _) = get_slice(mmap, offset as usize, len)?;

        Ok(account_block)
    }

    /// Returns the size of one index entry.
    pub fn entry_size(&self) -> usize {
        match self {
            Self::AddressAndOffset => std::mem::size_of::<Pubkey>() + std::mem::size_of::<u64>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::tiered_storage::file::TieredStorageFile, memmap2::MmapOptions, rand::Rng,
        std::fs::OpenOptions, tempfile::TempDir,
    };

    #[test]
    fn test_address_and_offset_indexer() {
        const ENTRY_COUNT: usize = 100;
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_address_and_offset_indexer");
        let addresses: Vec<_> = std::iter::repeat_with(Pubkey::new_unique)
            .take(ENTRY_COUNT)
            .collect();
        let mut rng = rand::thread_rng();
        let mut block_offset = 0;
        let index_entries: Vec<_> = addresses
            .iter()
            .map(|address| {
                if rng.gen_bool(0.5) {
                    block_offset += rng.gen_range(1, 128) * 8;
                }
                AccountIndexWriterEntry {
                    address,
                    block_offset,
                    intra_block_offset: 0,
                }
            })
            .collect();

        let account_blocks_size = block_offset + rng.gen_range(1, 128) * 8;
        let indexer = AccountIndex::AddressAndOffset;
        let footer = TieredStorageFooter {
            account_entry_count: ENTRY_COUNT as u32,
            // the account index block locates right after account blocks
            account_index_offset: account_blocks_size,
            // the owners block locates right after the account index block
            owners_offset: account_blocks_size + (indexer.entry_size() * ENTRY_COUNT) as u64,
            ..TieredStorageFooter::default()
        };

        {
            let file = TieredStorageFile::new_writable(&path).unwrap();
            let test_account_blocks: Vec<u8> =
                (0..account_blocks_size).map(|i| (i % 256) as u8).collect();
            file.write_bytes(&test_account_blocks).unwrap();
            indexer.write_index_block(&file, &index_entries).unwrap();
        }

        let indexer = AccountIndex::AddressAndOffset;
        let file = OpenOptions::new()
            .read(true)
            .create(false)
            .open(&path)
            .unwrap();
        let map = unsafe { MmapOptions::new().map(&file).unwrap() };

        let mut block_ending_offset = account_blocks_size;
        let (last_block_offset, _) = indexer
            .get_account_block_info(&map, &footer, ENTRY_COUNT - 1)
            .unwrap();
        let mut prev_block_offset = last_block_offset;

        for (i, index_entry) in index_entries.iter().enumerate().rev() {
            let (block_offset, block_size) =
                indexer.get_account_block_info(&map, &footer, i).unwrap();
            assert_eq!(index_entry.block_offset, block_offset,);
            if block_offset != prev_block_offset {
                block_ending_offset = prev_block_offset;
                prev_block_offset = block_offset;
            }
            assert_eq!(block_size as u64, block_ending_offset - block_offset);
            let account_block = indexer.get_account_block(&map, &footer, i).unwrap();
            assert_eq!(
                account_block,
                (block_offset..block_ending_offset)
                    .map(|i| (i % 256) as u8)
                    .collect::<Vec<_>>()
            );
        }
    }
}
