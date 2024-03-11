use {
    super::{error::TieredStorageError, TieredStorageResult},
    bytemuck::{AnyBitPattern, NoUninit, Pod, Zeroable},
    std::{
        fs::{File, OpenOptions},
        io::{Read, Result as IoResult, Seek, SeekFrom, Write},
        mem,
        path::Path,
    },
};

/// The ending 8 bytes of a valid tiered account storage file.
pub const FOOTER_MAGIC_NUMBER: u64 = 0x502A2AB5; // SOLALABS -> SOLANA LABS

#[derive(Debug, PartialEq, Eq, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct TieredStorageMagicNumber(pub u64);

// Ensure there are no implicit padding bytes
const _: () = assert!(std::mem::size_of::<TieredStorageMagicNumber>() == 8);

impl Default for TieredStorageMagicNumber {
    fn default() -> Self {
        Self(FOOTER_MAGIC_NUMBER)
    }
}

#[derive(Debug)]
pub struct TieredStorageFile(pub File);

impl TieredStorageFile {
    pub fn new_readonly(file_path: impl AsRef<Path>) -> TieredStorageResult<Self> {
        let tiered_storage_file = Self(
            OpenOptions::new()
                .read(true)
                .create(false)
                .open(&file_path)?,
        );

        tiered_storage_file.check_magic_number()?;

        Ok(tiered_storage_file)
    }

    pub fn new_writable(file_path: impl AsRef<Path>) -> IoResult<Self> {
        Ok(Self(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(file_path)?,
        ))
    }

    fn check_magic_number(&self) -> TieredStorageResult<()> {
        self.seek_from_end(-(std::mem::size_of::<TieredStorageMagicNumber>() as i64))?;
        let mut magic_number = TieredStorageMagicNumber::zeroed();
        self.read_pod(&mut magic_number)?;
        if magic_number != TieredStorageMagicNumber::default() {
            return Err(TieredStorageError::MagicNumberMismatch(
                TieredStorageMagicNumber::default().0,
                magic_number.0,
            ));
        }
        Ok(())
    }

    /// Writes `value` to the file.
    ///
    /// `value` must be plain ol' data.
    pub fn write_pod<T: NoUninit>(&self, value: &T) -> IoResult<usize> {
        // SAFETY: Since T is NoUninit, it does not contain any uninitialized bytes.
        unsafe { self.write_type(value) }
    }

    /// Writes `value` to the file.
    ///
    /// Prefer `write_pod` when possible, because `write_value` may cause
    /// undefined behavior if `value` contains uninitialized bytes.
    ///
    /// # Safety
    ///
    /// Caller must ensure casting T to bytes is safe.
    /// Refer to the Safety sections in std::slice::from_raw_parts()
    /// and bytemuck's Pod and NoUninit for more information.
    pub unsafe fn write_type<T>(&self, value: &T) -> IoResult<usize> {
        let ptr = value as *const _ as *const u8;
        let bytes = unsafe { std::slice::from_raw_parts(ptr, mem::size_of::<T>()) };
        self.write_bytes(bytes)
    }

    /// Reads a value of type `T` from the file.
    ///
    /// Type T must be plain ol' data.
    pub fn read_pod<T: NoUninit + AnyBitPattern>(&self, value: &mut T) -> IoResult<()> {
        // SAFETY: Since T is AnyBitPattern, it is safe to cast bytes to T.
        unsafe { self.read_type(value) }
    }

    /// Reads a value of type `T` from the file.
    ///
    /// Prefer `read_pod()` when possible, because `read_type()` may cause
    /// undefined behavior.
    ///
    /// # Safety
    ///
    /// Caller must ensure casting bytes to T is safe.
    /// Refer to the Safety sections in std::slice::from_raw_parts()
    /// and bytemuck's Pod and AnyBitPattern for more information.
    pub unsafe fn read_type<T>(&self, value: &mut T) -> IoResult<()> {
        let ptr = value as *mut _ as *mut u8;
        // SAFETY: The caller ensures it is safe to cast bytes to T,
        // we ensure the size is safe by querying T directly,
        // and Rust ensures ptr is aligned.
        let bytes = unsafe { std::slice::from_raw_parts_mut(ptr, mem::size_of::<T>()) };
        self.read_bytes(bytes)
    }

    pub fn seek(&self, offset: u64) -> IoResult<u64> {
        (&self.0).seek(SeekFrom::Start(offset))
    }

    pub fn seek_from_end(&self, offset: i64) -> IoResult<u64> {
        (&self.0).seek(SeekFrom::End(offset))
    }

    pub fn write_bytes(&self, bytes: &[u8]) -> IoResult<usize> {
        (&self.0).write_all(bytes)?;

        Ok(bytes.len())
    }

    pub fn read_bytes(&self, buffer: &mut [u8]) -> IoResult<()> {
        (&self.0).read_exact(buffer)
    }
}

#[cfg(test)]
mod tests {
    use {super::TieredStorageFile, tempfile::TempDir};

    #[test]
    #[should_panic(expected = "MagicNumberMismatch")]
    fn test_magic_number() {
        // Generate a new temp path that is guaranteed to NOT already have a file.
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_magic_number");
        {
            let file = TieredStorageFile::new_writable(&path).unwrap();
            let unmagic_number: u64 = 0x12345678;
            file.write_pod(&unmagic_number).unwrap();
        }
        TieredStorageFile::new_readonly(&path).unwrap();
    }
}
