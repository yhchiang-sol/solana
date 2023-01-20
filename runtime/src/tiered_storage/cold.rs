use {
    crate::{
        account_storage::meta::{StoredAccountMeta},
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        append_vec::MatchAccountOwnerError,
        tiered_storage::{
            data_block::{AccountDataBlock, AccountDataBlockFormat},
            file::TieredStorageFile,
            footer::TieredStorageFooter,
            reader::{TieredAccountMeta, TieredStorageReader},
            meta_entries::{AccountMetaStorageEntry},
        },
    },
    solana_sdk::{
        pubkey::Pubkey,
    },
    std::{collections::HashMap, path::Path},
};

#[derive(Debug)]
pub struct ColdStorageReader {
    pub(crate) footer: TieredStorageFooter,
    pub(crate) metas: Vec<AccountMetaStorageEntry>,
    accounts: Vec<Pubkey>,
    owners: Vec<Pubkey>,
    data_blocks: HashMap<u64, Vec<u8>>,
}

impl ColdStorageReader {
    pub fn new_from_file(file_path: impl AsRef<Path>) -> std::io::Result<TieredStorageReader> {
        let storage = TieredStorageFile::new(file_path, false /* create */);
        let footer = ColdReaderBuilder::read_footer_block(&storage)?;

        let metas = ColdReaderBuilder::read_account_metas_block(&storage, &footer)?;
        let accounts = ColdReaderBuilder::read_account_addresses_block(&storage, &footer)?;
        let owners = ColdReaderBuilder::read_owners_block(&storage, &footer)?;
        let data_blocks = ColdReaderBuilder::read_data_blocks(&storage, &footer, &metas)?;

        Ok(TieredStorageReader::Cold(ColdStorageReader {
            footer,
            metas,
            accounts,
            owners,
            data_blocks,
        }))
    }

    pub fn num_accounts(&self) -> usize {
        self.footer.account_meta_count.try_into().unwrap()
    }

    fn multiplied_index_to_index(multiplied_index: usize) -> usize {
        // This is a temporary workaround to work with existing AccountInfo
        // implementation that ties to AppendVec with the assumption that the offset
        // is a multiple of ALIGN_BOUNDARY_OFFSET, while tiered storage actually talks
        // about index instead of offset.
        multiplied_index / ALIGN_BOUNDARY_OFFSET
    }

    pub fn account_matches_owners(
        &self,
        multiplied_index: usize,
        owners: &[&Pubkey],
    ) -> Result<usize, MatchAccountOwnerError> {
        let index = Self::multiplied_index_to_index(multiplied_index);
        if index >= self.metas.len() {
            return Err(MatchAccountOwnerError::UnableToLoad);
        }

        owners
            .iter()
            .position(|entry| &&self.owners[self.metas[index].owner_local_id() as usize] == entry)
            .ok_or(MatchAccountOwnerError::NoMatch)
    }

    pub fn get_account<'a>(
        &'a self,
        multiplied_index: usize,
    ) -> Option<(StoredAccountMeta<'a>, usize)> {
        let index = Self::multiplied_index_to_index(multiplied_index);
        if index >= self.metas.len() {
            return None;
        }
        if let Some(data_block) = self.data_blocks.get(&self.metas[index].block_offset()) {
            return Some((
                StoredAccountMeta::Tiered(TieredAccountMeta {
                    meta: &self.metas[index],
                    pubkey: &self.accounts[index],
                    owner: &self.owners[self.metas[index].owner_local_id() as usize],
                    index: multiplied_index,
                    data_block: data_block,
                }),
                multiplied_index + ALIGN_BOUNDARY_OFFSET,
            ));
        }
        None
    }
}

pub(crate) struct ColdReaderBuilder {}

impl ColdReaderBuilder {

    fn read_footer_block(storage: &TieredStorageFile) -> std::io::Result<TieredStorageFooter> {
        TieredStorageFooter::new_from_footer_block(&storage)
    }

    fn read_account_metas_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
    ) -> std::io::Result<Vec<AccountMetaStorageEntry>> {
        let mut metas: Vec<AccountMetaStorageEntry> =
            Vec::with_capacity(footer.account_meta_count as usize);

        (&storage).seek(footer.account_metas_offset)?;

        for _ in 0..footer.account_meta_count {
            metas.push(AccountMetaStorageEntry::new_from_file(&storage)?);
        }

        Ok(metas)
    }

    fn read_account_addresses_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
    ) -> std::io::Result<Vec<Pubkey>> {
        Self::read_pubkeys_block(
            storage,
            footer.account_pubkeys_offset,
            footer.account_meta_count,
        )
    }

    fn read_owners_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
    ) -> std::io::Result<Vec<Pubkey>> {
        Self::read_pubkeys_block(storage, footer.owners_offset, footer.owner_count)
    }

    fn read_pubkeys_block(
        storage: &TieredStorageFile,
        offset: u64,
        count: u32,
    ) -> std::io::Result<Vec<Pubkey>> {
        let mut addresses: Vec<Pubkey> = Vec::with_capacity(count as usize);
        (&storage).seek(offset)?;
        for _ in 0..count {
            let mut pubkey = Pubkey::default();
            (&storage).read_type(&mut pubkey)?;
            addresses.push(pubkey);
        }

        Ok(addresses)
    }

    pub fn read_data_blocks(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
        metas: &Vec<AccountMetaStorageEntry>,
    ) -> std::io::Result<HashMap<u64, Vec<u8>>> {
        let count = footer.account_meta_count as usize;
        let mut data_blocks = HashMap::<u64, Vec<u8>>::new();
        for i in 0..count {
            Self::update_data_block_map(&mut data_blocks, storage, footer, metas, i)?;
        }
        Ok(data_blocks)
    }

    fn update_data_block_map(
        data_blocks: &mut HashMap<u64, Vec<u8>>,
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
        metas: &Vec<AccountMetaStorageEntry>,
        index: usize,
    ) -> std::io::Result<()> {
        let block_offset = &metas[index].block_offset();
        if !data_blocks.contains_key(&block_offset) {
            let data_block = Self::read_data_block(storage, footer, metas, index).unwrap();

            data_blocks.insert(metas[index].block_offset(), data_block);
        }
        Ok(())
    }

    pub fn read_data_block(
        storage: &TieredStorageFile,
        footer: &TieredStorageFooter,
        metas: &Vec<AccountMetaStorageEntry>,
        index: usize,
    ) -> std::io::Result<Vec<u8>> {
        let compressed_block_size = Self::get_compressed_block_size(footer, metas, index) as usize;

        (&storage).seek(metas[index].block_offset())?;

        let mut buffer: Vec<u8> = vec![0; compressed_block_size];
        (&storage).read_bytes(&mut buffer)?;

        // TODO(yhchiang): encoding from footer
        Ok(AccountDataBlock::decode(
            AccountDataBlockFormat::Lz4,
            &buffer[..],
        )?)
    }

    pub(crate) fn get_compressed_block_size(
        footer: &TieredStorageFooter,
        metas: &Vec<AccountMetaStorageEntry>,
        index: usize,
    ) -> usize {
        let mut block_size = footer.account_metas_offset - metas[index].block_offset();

        for i in index..metas.len() {
            if metas[i].block_offset() == metas[index].block_offset() {
                continue;
            }
            block_size = metas[i].block_offset() - metas[index].block_offset();
            break;
        }

        block_size.try_into().unwrap()
    }
}

