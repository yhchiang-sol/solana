//! docs/src/proposals/append-vec-storage.md

use {
    crate::tiered_storage::{
        file::TieredStorageFile, footer::TieredStorageFooter, mmap_utils::get_type,
        TieredStorageResult,
    },
    memmap2::Mmap,
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};

/// The in-memory struct for managing account owners used in the
/// write path of the tiered-storage.
pub struct AccountOwnersTable<'address> {
    pub owners_vec: Vec<&'address Pubkey>,
    pub owners_map: HashMap<&'address Pubkey, u32>,
}

impl<'address> AccountOwnersTable<'address> {
    /// Create a new instance of AccountOwnerTable
    pub fn new() -> Self {
        Self {
            owners_vec: vec![],
            owners_map: HashMap::new(),
        }
    }

    /// Insert the specified address to the AccountOwnerTable if it does not
    /// previously exist.  In either case, the function returns the index
    /// of the specified owner address.
    pub fn try_insert(&mut self, address: &'address Pubkey) -> u32 {
        if let Some(index) = self.owners_map.get(address) {
            return *index;
        }
        let index: u32 = self.owners_vec.len().try_into().unwrap();
        self.owners_vec.push(address);
        self.owners_map.insert(address, index);

        index
    }

    pub fn len(&self) -> usize {
        self.owners_vec.len()
    }
}

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
pub enum OwnersBlockFormat {
    #[default]
    LocalIndex = 0,
}

impl OwnersBlockFormat {
    /// Persists the given owners_table to the specified tiered storage file
    /// and returns the total number of bytes written.
    pub fn write_owners_block(
        &self,
        file: &TieredStorageFile,
        owners_table: &AccountOwnersTable,
    ) -> TieredStorageResult<usize> {
        match self {
            Self::LocalIndex => {
                let mut stored_size = 0;
                for address in &owners_table.owners_vec {
                    println!("write_owners_block {address}");
                    stored_size += file.write_type(*address)?;
                }
                Ok(stored_size)
            }
        }
    }

    /// Returns the owner address associated with the specified owner index.
    ///
    /// The owner index should be obtained via the TieredAccountMeta instance.
    pub fn get_owner_address<'a>(
        &self,
        mmap: &'a Mmap,
        footer: &TieredStorageFooter,
        owner_index: usize,
    ) -> TieredStorageResult<&'a Pubkey> {
        match self {
            Self::LocalIndex => {
                let offset =
                    footer.owners_offset as usize + std::mem::size_of::<Pubkey>() * owner_index;
                let (owner_address, _) = get_type::<Pubkey>(mmap, offset)?;
                Ok(owner_address)
            }
        }
    }
}
