use {
    crate::{
        account_storage::meta::{StoredAccountMeta, StoredMeta, StoredMetaWriteVersion},
        append_vec::{MatchAccountOwnerError},
        tiered_storage::{
            cold::ColdStorageReader,
            meta_entries::{AccountMetaFlags, AccountMetaStorageEntry},
        },
    },
    solana_sdk::{
        account::{Account, AccountSharedData, ReadableAccount},
        hash::Hash,
        pubkey::Pubkey,
        stake_history::Epoch,
    },
    std::path::Path,
};

#[derive(Debug)]
pub enum TieredStorageReader {
    Cold(ColdStorageReader),
    /*
    pub(crate) footer: TieredStorageFooter,
    pub(crate) metas: Vec<AccountMetaStorageEntry>,
    pub(crate) accounts: Vec<Pubkey>,
    pub(crate) owners: Vec<Pubkey>,
    pub(crate) data_blocks: HashMap<u64, Vec<u8>>,
    */
}

impl TieredStorageReader {
    pub fn new_from_path<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        // TODO(yhchiang): invoke the right builder based on the footer information
        ColdStorageReader::new_from_file(path)
    }

    pub fn num_accounts(&self) -> usize {
        match self {
            Self::Cold(cs) => cs.num_accounts(),
        }
    }

    pub fn account_matches_owners(
        &self,
        multiplied_index: usize,
        owners: &[&Pubkey],
    ) -> Result<usize, MatchAccountOwnerError> {
        match self {
            Self::Cold(cs) => cs.account_matches_owners(multiplied_index, owners),
        }
    }

    pub fn get_account<'a>(
        &'a self,
        multiplied_index: usize,
    ) -> Option<(StoredAccountMeta<'a>, usize)> {
        match self {
            Self::Cold(cs) => cs.get_account(multiplied_index),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
#[allow(dead_code)]
pub struct TieredAccountMeta<'a> {
    pub(crate) meta: &'a AccountMetaStorageEntry,
    pub(crate) pubkey: &'a Pubkey,
    pub(crate) owner: &'a Pubkey,
    pub(crate) index: usize,
    // this data block may be shared with other accounts
    pub(crate) data_block: &'a [u8],
}

#[allow(dead_code)]
impl<'a> TieredAccountMeta<'a> {
    pub fn pubkey(&self) -> &'a Pubkey {
        &self.pubkey
    }

    pub fn hash(&self) -> &'a Hash {
        self.meta.account_hash(self.data_block)
    }

    pub fn offset(&self) -> usize {
        self.index
    }

    pub fn data(&self) -> &'a [u8] {
        self.meta.account_data(self.data_block)
    }

    pub fn data_len(&self) -> u64 {
        self.meta.account_data(self.data_block).len() as u64
    }

    pub fn stored_size(&self) -> usize {
        self.data_len() as usize / 2
            + std::mem::size_of::<AccountMetaStorageEntry>()
            + std::mem::size_of::<Pubkey>() // account's pubkey
            + std::mem::size_of::<Pubkey>() // owner's pubkey
    }

    pub fn clone_account(&self) -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports: self.lamports(),
            owner: *self.owner(),
            executable: self.executable(),
            rent_epoch: self.rent_epoch(),
            data: self.data().to_vec(),
        })
    }

    pub fn write_version(&self) -> StoredMetaWriteVersion {
        if let Some(write_version) = self.meta.write_version(self.data_block) {
            return write_version;
        }
        0
    }

    ///////////////////////////////////////////////////////////////////////////
    // Unimlpemented

    pub fn meta(&self) -> &StoredMeta {
        unimplemented!();
    }

    pub fn set_meta(&mut self, _meta: &'a StoredMeta) {
        unimplemented!();
    }

    pub(crate) fn sanitize(&self) -> bool {
        unimplemented!();
    }
}

impl<'a> ReadableAccount for TieredAccountMeta<'a> {
    fn lamports(&self) -> u64 {
        self.meta.lamports()
    }
    fn owner(&self) -> &'a Pubkey {
        self.owner
    }
    fn executable(&self) -> bool {
        self.meta.flags_get(AccountMetaFlags::EXECUTABLE)
    }
    fn rent_epoch(&self) -> Epoch {
        if let Some(rent_epoch) = self.meta.rent_epoch(self.data_block) {
            return rent_epoch;
        }
        std::u64::MAX
    }
    fn data(&self) -> &'a [u8] {
        self.meta.account_data(self.data_block)
    }
}

/*
    #[test]
    fn test_account_pubkeys_block() {
        let path = get_append_vec_path("test_account_pubkeys_block");
        let mut expected_pubkeys: Vec<Pubkey> = vec![];
        const ENTRY_COUNT: u32 = 1024;

        {
            let ads = TieredStorageWriter::new(&path.path);
            let mut footer = TieredStorageFooter::new();
            let mut cursor = 0;
            for _ in 0..ENTRY_COUNT {
                expected_pubkeys.push(Pubkey::new_unique());
            }
            ads.write_account_pubkeys_block(&mut cursor, &mut footer, &expected_pubkeys)
                .unwrap();
        }

        let ads = TieredStorage::new_for_test(&path.path, false);
        let pubkeys: Vec<Pubkey> = ads.read_pubkeys_block(0, ENTRY_COUNT).unwrap();
        assert_eq!(expected_pubkeys, pubkeys);
    }
*/
