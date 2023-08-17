//! docs/src/proposals/append-vec-storage.md

use {
    crate::{
        account_storage::meta::{StorableAccountsWithHashesAndWriteVersions, StoredAccountInfo},
        storable_accounts::StorableAccounts,
        tiered_storage::{
            error::TieredStorageError, file::TieredStorageFile, footer::TieredStorageFooter,
            TieredStorageFormat, TieredStorageResult,
        },
    },
    solana_sdk::{account::ReadableAccount, hash::Hash},
    std::{borrow::Borrow, path::Path},
};

const EMPTY_ACCOUNT_DATA: [u8; 0] = [0u8; 0];
const PADDING: [u8; 8] = [0x8; 8];

/// A helper function that extracts the lamports, rent epoch, and account data
/// from the specified ReadableAccount, or returns the default of these values
/// when the account is None (e.g. a zero-lamport account).
fn get_account_fields<T: ReadableAccount + Sync>(account: Option<&T>) -> (u64, u64, &[u8]) {
    if let Some(account) = account {
        return (account.lamports(), account.rent_epoch(), account.data());
    }

    (0, u64::MAX, &EMPTY_ACCOUNT_DATA)
}

#[derive(Debug)]
pub struct TieredStorageWriter<'format> {
    storage: TieredStorageFile,
    format: &'format TieredStorageFormat,
}

impl<'format> TieredStorageWriter<'format> {
    pub fn new(
        file_path: impl AsRef<Path>,
        format: &'format TieredStorageFormat,
    ) -> TieredStorageResult<Self> {
        Ok(Self {
            storage: TieredStorageFile::new_writable(file_path)?,
            format,
        })
    }

    pub fn write_accounts<
        'a,
        'b,
        T: ReadableAccount + Sync,
        U: StorableAccounts<'a, T>,
        V: Borrow<Hash>,
    >(
        &self,
        accounts: &StorableAccountsWithHashesAndWriteVersions<'a, 'b, T, U, V>,
        skip: usize,
    ) -> TieredStorageResult<Vec<StoredAccountInfo>> {
        let mut footer = TieredStorageFooter {
            account_meta_format: self.format.account_meta_format,
            owners_block_format: self.format.owners_block_format,
            account_block_format: self.format.account_block_format,
            account_index_format: self.format.account_index_format,
            ..TieredStorageFooter::default()
        };

        footer.account_entry_count = accounts.accounts.len().saturating_sub(skip) as u32;
        footer.write_footer_block(&self.storage)?;

        Err(TieredStorageError::Unsupported())
    }
}
