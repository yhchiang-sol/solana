use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    mem,
    path::Path,
};

#[derive(Debug)]
pub struct TieredStorageFile {
    pub file: File,
}

impl TieredStorageFile {
    /// Creates a tiered-storage file.
    /// If the create flag is false, it will open an existing file
    /// in read-only mode.
    pub fn new<P: AsRef<Path>>(file_path: P, create: bool) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(create)
            .create(create)
            .open(file_path.as_ref())
            .map_err(|e| {
                panic!(
                    "Unable to {} data file {} in current dir({:?}): {:?}",
                    if create { "create" } else { "open" },
                    file_path.as_ref().display(),
                    std::env::current_dir(),
                    e
                );
            })
            .unwrap();
        Self { file }
    }

    pub fn write_type<T>(&self, value: &T) -> Result<usize, std::io::Error> {
        unsafe {
            let ptr =
                std::slice::from_raw_parts((value as *const T) as *const u8, mem::size_of::<T>());
            (&self.file).write_all(ptr)?;
        }
        Ok(std::mem::size_of::<T>())
    }

    pub fn read_type<T>(&self, value: &mut T) -> Result<(), std::io::Error> {
        unsafe {
            let ptr =
                std::slice::from_raw_parts_mut((value as *mut T) as *mut u8, mem::size_of::<T>());
            (&self.file).read_exact(ptr)?;
        }
        Ok(())
    }

    pub fn seek(&self, offset: u64) -> Result<u64, std::io::Error> {
        (&self.file).seek(SeekFrom::Start(offset))
    }

    pub fn seek_from_end(&self, offset: i64) -> Result<u64, std::io::Error> {
        (&self.file).seek(SeekFrom::End(offset))
    }

    pub fn write_bytes(&self, bytes: &[u8]) -> Result<usize, std::io::Error> {
        (&self.file).write_all(bytes)?;

        Ok(bytes.len())
    }

    pub fn read_bytes(&self, buffer: &mut [u8]) -> Result<(), std::io::Error> {
        (&self.file).read_exact(buffer)?;

        Ok(())
    }
}
