use {
    crate::{
        account_storage::meta::{
            StorableAccountsWithHashesAndWriteVersions, StoredAccountInfo, StoredAccountMeta,
        },
        accounts_hash::AccountHash,
        append_vec::{AppendVec, AppendVecError},
        storable_accounts::StorableAccounts,
        tiered_storage::{
            error::TieredStorageError, hot::HOT_FORMAT, index::IndexOffset, TieredStorage,
        },
    },
    solana_sdk::{account::ReadableAccount, clock::Slot, pubkey::Pubkey},
    std::{
        borrow::Borrow,
        mem,
        path::{Path, PathBuf},
    },
    thiserror::Error,
};

// Data placement should be aligned at the next boundary. Without alignment accessing the memory may
// crash on some architectures.
pub const ALIGN_BOUNDARY_OFFSET: usize = mem::size_of::<u64>();
#[macro_export]
macro_rules! u64_align {
    ($addr: expr) => {
        ($addr + (ALIGN_BOUNDARY_OFFSET - 1)) & !(ALIGN_BOUNDARY_OFFSET - 1)
    };
}

#[derive(Error, Debug)]
/// An enum for AccountsFile related errors.
pub enum AccountsFileError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("AppendVecError: {0}")]
    AppendVecError(#[from] AppendVecError),

    #[error("TieredStorageError: {0}")]
    TieredStorageError(#[from] TieredStorageError),
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum MatchAccountOwnerError {
    #[error("The account owner does not match with the provided list")]
    NoMatch,
    #[error("Unable to load the account")]
    UnableToLoad,
}

pub type Result<T> = std::result::Result<T, AccountsFileError>;

#[derive(Debug)]
/// An enum for accessing an accounts file which can be implemented
/// under different formats.
pub enum AccountsFile {
    AppendVec(AppendVec),
    TieredHot(TieredStorage),
}

impl AccountsFile {
    /// Create an AccountsFile instance from the specified path.
    ///
    /// The second element of the returned tuple is the number of accounts in the
    /// accounts file.
    pub fn new_from_file(path: impl AsRef<Path>, current_len: usize) -> Result<(Self, usize)> {
        match TieredStorage::new_readonly(path.as_ref()) {
            Ok(tiered_storage) => {
                // we are doing unwrap here because TieredStorage::new_readonly() is
                // guaranteed to have a valid reader instance when opening with
                // new_readonly.
                let num_accounts = tiered_storage.reader().unwrap().num_accounts();
                Ok((Self::TieredHot(tiered_storage), num_accounts))
            }
            Err(TieredStorageError::MagicNumberMismatch(_, _)) => {
                // In case of MagicNumberMismatch, we can assume that this is not
                // a tiered-storage file.
                let (av, num_accounts) = AppendVec::new_from_file(path, current_len)?;
                Ok((Self::AppendVec(av), num_accounts))
            }
            Err(e) => Err(AccountsFileError::TieredStorageError(e)),
        }
    }

    pub fn flush(&self) -> Result<()> {
        match self {
            Self::AppendVec(av) => av.flush(),
            Self::TieredHot(_) => Ok(()),
        }
    }

    pub fn reset(&self) {
        match self {
            Self::AppendVec(av) => av.reset(),
            Self::TieredHot(_) => {}
        }
    }

    pub fn remaining_bytes(&self) -> u64 {
        match self {
            Self::AppendVec(av) => av.remaining_bytes(),
            Self::TieredHot(ts) => {
                if ts.is_read_only() {
                    0
                } else {
                    u64::MAX
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::AppendVec(av) => av.len(),
            Self::TieredHot(ts) => ts.file_size().unwrap() as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::AppendVec(av) => av.is_empty(),
            Self::TieredHot(ts) => ts.file_size().unwrap() == 0,
        }
    }

    pub fn capacity(&self) -> u64 {
        match self {
            Self::AppendVec(av) => av.capacity(),
            Self::TieredHot(ts) => {
                if ts.is_read_only() {
                    return ts.file_size().unwrap_or(0);
                }
                u64::MAX
            }
        }
    }

    pub fn is_recyclable(&self) -> bool {
        match self {
            Self::AppendVec(_) => true,
            Self::TieredHot(_) => false,
        }
    }

    pub fn file_name(slot: Slot, id: impl std::fmt::Display) -> String {
        format!("{slot}.{id}")
    }

    /// Return (account metadata, next_index) pair for the account at the
    /// specified `index` if any.  Otherwise return None.   Also return the
    /// index of the next entry.
    pub fn get_account(&self, index: usize) -> Option<(StoredAccountMeta<'_>, usize)> {
        match self {
            Self::AppendVec(av) => av.get_account(index),
            Self::TieredHot(ts) => {
                if let Some(reader) = ts.reader() {
                    return reader
                        .get_account(IndexOffset(index as u32))
                        .unwrap()
                        .map(|(metas, index_offset)| (metas, index_offset.0 as usize));
                }
                None
            }
        }
    }

    pub fn account_matches_owners(
        &self,
        offset: usize,
        owners: &[Pubkey],
    ) -> std::result::Result<usize, MatchAccountOwnerError> {
        match self {
            Self::AppendVec(av) => av.account_matches_owners(offset, owners),
            Self::TieredHot(ts) => {
                if let Some(reader) = ts.reader() {
                    return reader.account_matches_owners(IndexOffset(offset as u32), owners);
                }
                Err(MatchAccountOwnerError::UnableToLoad)
            }
        }
    }

    /// Return the path of the underlying account file.
    pub fn get_path(&self) -> PathBuf {
        match self {
            Self::AppendVec(av) => av.get_path(),
            Self::TieredHot(ts) => ts.path().to_path_buf(),
        }
    }

    /// Return iterator for account metadata
    pub fn account_iter(&self) -> AccountsFileIter {
        AccountsFileIter::new(self)
    }

    /// Return a vector of account metadata for each account, starting from `offset`.
    pub fn accounts(&self, offset: usize) -> Vec<StoredAccountMeta> {
        match self {
            Self::AppendVec(av) => av.accounts(offset),
            Self::TieredHot(ts) => {
                if let Some(reader) = ts.reader() {
                    // A conversion is needed here as TieredStorage uses reduced-offsets
                    // while AccountsDb uses non-reduced-offsets instead.
                    return reader
                        .accounts(IndexOffset(AccountInfo::get_reduced_offset(offset)))
                        .unwrap();
                }
                vec![]
            }
        }
    }

    /// Copy each account metadata, account and hash to the internal buffer.
    /// If there is no room to write the first entry, None is returned.
    /// Otherwise, returns the starting offset of each account metadata.
    /// Plus, the final return value is the offset where the next entry would be appended.
    /// So, return.len() is 1 + (number of accounts written)
    /// After each account is appended, the internal `current_len` is updated
    /// and will be available to other threads.
    pub fn append_accounts<
        'a,
        'b,
        T: ReadableAccount + Sync,
        U: StorableAccounts<'a, T>,
        V: Borrow<AccountHash>,
    >(
        &self,
        accounts: &StorableAccountsWithHashesAndWriteVersions<'a, 'b, T, U, V>,
        skip: usize,
    ) -> Option<Vec<StoredAccountInfo>> {
        match self {
            Self::AppendVec(av) => av.append_accounts(accounts, skip),
            Self::TieredHot(ts) => ts.write_accounts(accounts, skip, &HOT_FORMAT).ok(),
        }
    }
}

pub struct AccountsFileIter<'a> {
    file_entry: &'a AccountsFile,
    offset: usize,
}

impl<'a> AccountsFileIter<'a> {
    pub fn new(file_entry: &'a AccountsFile) -> Self {
        Self {
            file_entry,
            offset: 0,
        }
    }
}

impl<'a> Iterator for AccountsFileIter<'a> {
    type Item = StoredAccountMeta<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((account, next_offset)) = self.file_entry.get_account(self.offset) {
            self.offset = next_offset;
            Some(account)
        } else {
            None
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::accounts_file::AccountsFile;
    impl AccountsFile {
        pub(crate) fn set_current_len_for_tests(&self, len: usize) {
            match self {
                Self::AppendVec(av) => av.set_current_len_for_tests(len),
                Self::TieredHot(_) => {}
            }
        }
    }
}
