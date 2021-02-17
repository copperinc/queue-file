// Java version: Copyright (C) 2010 Square, Inc.
// Rust version: Copyright (C) 2019 ING Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `queue-file` crate is a feature complete and binary compatible port of `QueueFile` class from
//! Tape2 by Square, Inc. Check the original project [here](https://github.com/square/tape).

#[cfg(test)]
extern crate pretty_assertions;

use snafu::{ensure, Snafu};
use std::cmp::min;
use std::fs::{rename, File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;

use bytes::{ Buf, BufMut, BytesMut, IntoBuf };
use crc::{crc32, Hasher32};


#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    Io { source: std::io::Error },
    #[snafu(display("too many elements"))]
    TooManyElements {},
    #[snafu(display("element too big"))]
    ElementTooBig {},
    #[snafu(display("no space in queue"))]
    NoSpace {},
    #[snafu(display("corrupted file: {}", msg))]
    CorruptedFile { msg: String },
    #[snafu(display(
        "unsupported version {}. supported versions is {} and legacy",
        detected,
        supported
    ))]
    UnsupportedVersion { detected: u32, supported: u32 },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// QueueFile is a lightning-fast, transactional, file-based FIFO.
///
/// Addition and removal from an instance is an O(1) operation and is atomic.
/// Writes are synchronous by default; data will be written to disk before an operation returns.
///
/// The underlying file. Uses a ring buffer to store entries. Designed so that a modification
/// isn't committed or visible until we write the header. The header is much smaller than a
/// segment. So long as the underlying file system supports atomic segment writes, changes to the
/// queue are atomic. Storing the file length ensures we can recover from a failed expansion
/// (i.e. if setting the file length succeeds but the process dies before the data can be copied).
///
/// # Example
/// ```
/// use queue_file::QueueFile;
///
/// let mut qf = QueueFile::open("example.qf")
///     .expect("cannot open queue file");
/// let data = "Welcome to QueueFile!".as_bytes();
///
/// qf.add(&data).expect("add failed");
///
/// if let Ok(Some(bytes)) = qf.peek() {
///     assert_eq!(data, bytes.as_ref());
/// }
///
/// qf.remove().expect("remove failed");
/// ```
/// # File format
///
/// ```text
///   16-32 bytes      Header
///   ...              Data
/// ```
/// This implementation supports two versions of the header format.
/// ```text
/// Versioned Header (32 bytes):
///   16 bits          flags: 0x8000, raw_blkdev 0x8001
///   16 bits          Version, 0, 1(versioned), 2(crc)
///   8 bytes          File length
///   4 bytes          Element count
///   8 bytes          Head element position
///   8 bytes          Tail element position
///
/// Legacy Header (16 bytes):
///   1 bit            Legacy indicator, always 0
///   31 bits          File length
///   4 bytes          Element count
///   4 bytes          Head element position
///   4 bytes          Tail element position
/// ```
/// Each element stored is represented by:
/// ```text
/// Element:
///   4 bytes          Data length
///   ...              Data
/// ```
#[derive(Debug)]
pub struct QueueFile {
    file: File,
    /// v0 is unversioned header, v1 is versioned format, v2 adds crc checking
    version: u16,
    /// The header for the queue file
    header: Header,
    /// Cached file length. Always a power of 2.
    file_len: u64,
    /// Number of elements.
    elem_cnt: u32,
    /// Pointer to first (or eldest) element.
    first: Element,
    /// Pointer to last (or newest) element.
    last: Element,
    /// When true, removing an element will also overwrite data with zero bytes.
    /// It's true by default.
    overwrite_on_remove: bool,
    /// When true, operations are optimized for running the queue on a raw block device
    /// can also be used for fixed size files
    rawblkdev: bool,
    /// When true, every write to file will be followed by `sync_data()` call.
    /// It's true by default.
    sync_writes: bool,
    /// Buffer used by `transfer` function.
    transfer_buf: Box<[u8]>,
}


impl QueueFile {
    const INITIAL_LENGTH: u64 = 4096;
    const VERSION: u16 = 2;
    const ZEROES: [u8; 4096] = [0; 4096];

    fn init(path: &Path, len: u64, rawblkdev: bool, force_legacy: bool) -> Result<()> {

        if rawblkdev {
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            file.seek(SeekFrom::Start(0))?;

            let ver = if force_legacy { 0 } else { QueueFile::VERSION };
            let mut header = Header::new(ver);
            header.file_len = len;
            header.set_rawblkdev(rawblkdev);
            header.calc_bytes();

            file.write_all(header.hdr_buf.as_ref())?;


            file.sync_all()?;

        } else {
            let tmp_path = path.with_extension("tmp");

            // Use a temp file so we don't leave a partially-initialized file.
            {
                let mut file = OpenOptions::new().read(true).write(true).create(true).open(&tmp_path)?;
                file.set_len(len)?;
                file.seek(SeekFrom::Start(0))?;

                let ver = if force_legacy { 0 } else { QueueFile::VERSION };
                let mut header = Header::new(ver);
                header.file_len = len;
                header.calc_bytes();

                file.write_all(header.hdr_buf.as_ref())?;

                file.sync_all()?;
            }

            // A rename is atomic.
            rename(tmp_path, path)?;
        }
        Ok(())
    }

    /// initialize a queue-file, if a length is provided then fixed size raw
    /// block device storage is assumed
    pub fn create<P: AsRef<Path>>(path: P, len: Option<u64>) -> Result<QueueFile> {

        let force_legacy = false;
        let mut rawblkdev = false;

        let len = if let Some(len) = len {
            rawblkdev = true;
            len
        } else {
            QueueFile::INITIAL_LENGTH
        };
        
        Self::init(path.as_ref(), len, rawblkdev, force_legacy)?;
        Self::open_internal(path, false, false)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<QueueFile> {
        Self::open_internal(path, true, false)
    }

    pub fn open_legacy<P: AsRef<Path>>(path: P) -> Result<QueueFile> {
        Self::open_internal(path, true, true)
    }

    fn open_internal<P: AsRef<Path>>(
        path: P, overwrite_on_remove: bool, force_legacy: bool,
    ) -> Result<QueueFile> {
        //TODO: hmm think about initialization of file..
        if !path.as_ref().exists() {
            QueueFile::init(path.as_ref(), Self::INITIAL_LENGTH, false, force_legacy)?;
        }

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut buf = [0u8; 36];

        file.seek(SeekFrom::Start(0))?;
        let bytes_read = file.read(&mut buf)?;

        ensure!(bytes_read >= 4, CorruptedFile { msg: "file hdr ver too short" });
        
        // based on version check to see if a header exists
        let version: u16 = if force_legacy {
            0
        } else {
            // let mut buf = BytesMut::from(&buf[..]); // bytes 0.5 and above
            // let mut buf = std::io::Cursor::new(&buf);
            let mut buf = buf.into_buf();
            let _flags = buf.get_u16_be();
            buf.get_u16_be()
        };
       
        ensure!(bytes_read >= Header::version_len(version) as usize,
            CorruptedFile { msg: "file header too short" });

        let header = Header::from_bytes(&buf);
        if header.version > 1 && !header.verify_crc() {
            ensure!(false, CorruptedFile { msg: "header failed crc check" });
        }

        //TODO: if elem_cnt = 0 and version == 1, upgrade to version 2 header w/ crc
        //TODO: support v1 element headers
        ensure!(header.version == QueueFile::VERSION,
            UnsupportedVersion { 
                detected: header.version, 
                supported: QueueFile::VERSION});

        let rawblkdev = header.is_rawblkdev();

        if !rawblkdev {
            let real_file_len = file.metadata()?.len();

            ensure!(header.file_len <= real_file_len, CorruptedFile {
                msg: format!(
                    "file is truncated. expected length was {} but actual length is {}",
                    header.file_len, real_file_len
                    )
            });          
        
        }    

        let file_len = header.file_len;
        let elem_cnt = header.elem_cnt;

        ensure!(file_len >= header.hlen, CorruptedFile {
            msg: format!("length stored in header ({}) is invalid", file_len)
        });
        ensure!(header.head_pos <= file_len, CorruptedFile {
            msg: format!("position of the first element ({}) is beyond the file", header.head_pos)
        });
        ensure!(header.tail_pos <= file_len, CorruptedFile {
            msg: format!("position of the last element ({}) is beyond the file", header.tail_pos),
        });

        // unset overwrite_on_remove if the device is a raw blockdevice
        let overwrite_on_remove = if header.is_rawblkdev() {
            false
        } else {
            overwrite_on_remove
        };

        let mut queue_file = QueueFile {
            file,
            version: header.version,
            header,
            file_len,
            elem_cnt,
            first: Element::EMPTY,
            last: Element::EMPTY,
            overwrite_on_remove,
            rawblkdev,
            sync_writes: cfg!(not(test)),
            transfer_buf: vec![0u8; Self::TRANSFER_BUFFER_SIZE].into_boxed_slice(),
        };

        queue_file.first = queue_file.read_element(queue_file.header.head_pos)?;
        queue_file.last = queue_file.read_element(queue_file.header.tail_pos)?;

        // dbg!("open", queue_file.first, queue_file.last);
        Ok(queue_file)
    }

    /// Returns true if removing an element will also overwrite data with zero bytes.
    pub fn get_overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove
    }

    /// If set to true removing an element will also overwrite data with zero bytes.
    pub fn set_overwrite_on_remove(&mut self, value: bool) {
        self.overwrite_on_remove = value
    }

    /// Returns true if every write to file will be followed by `sync_data()` call.
    pub fn get_sync_writes(&self) -> bool {
        self.sync_writes
    }

    /// If set to true every write to file will be followed by `sync_data()` call.
    pub fn set_sync_writes(&mut self, value: bool) {
        self.sync_writes = value
    }

    /// Returns true if this queue contains no entries.
    pub fn is_empty(&self) -> bool {
        self.elem_cnt == 0
    }

    /// Returns the number of elements in this queue.
    pub fn size(&self) -> usize {
        self.elem_cnt as usize
    }

    /// Returns if the rawblockdev option is set
    pub fn rawblockdev(&self) -> bool {
        self.rawblkdev
    }

    /// Adds an element to the end of the queue.
    pub fn add(&mut self, buf: &[u8]) -> Result<()> {     
        // check if we can add 1 to elem_cnt
        const I32_MAX_USIZE: u32 = i32::max_value() as u32;
        ensure!(self.elem_cnt + 1 < I32_MAX_USIZE, TooManyElements {});

        let len = buf.len();

        ensure!(len <= i32::max_value() as usize, ElementTooBig {});

        if self.rawblkdev {
            let need_bytes = Element::HEADER_LENGTH_V2 as u64 + len as u64;
            ensure!(self.remaining_bytes() >= need_bytes, NoSpace);
        } else {
            self.expand_if_necessary(len)?;
        }

        // Insert a new element after the current last element.
        let was_empty = self.is_empty();
        let pos = if was_empty {
            if self.rawblkdev {
                if self.last.pos < self.header.hlen {
                    self.header.hlen
                } else {
                    self.last.pos
                }
            } else {
               self.header.hlen 
            }
        } else {
            self.wrap_pos(self.last.pos + Element::HEADER_LENGTH_V2 as u64 + self.last.len as u64)
        };

        let new_last = Element::new(pos, len);

        // Write length.
        self.ring_write(
            new_last.pos,
            &(len as u32).to_be_bytes(),
            0,
            std::mem::size_of::<u32>()
        )?;

        // Write crc
        let crc = Element::crc(new_last.len);
        self.ring_write(
            new_last.pos + std::mem::size_of::<u32>() as u64,
            &(crc.to_be_bytes()),
            0,
            std::mem::size_of::<u32>()
        )?;

        // Write data.
        self.ring_write(new_last.pos + Element::HEADER_LENGTH_V2 as u64, buf, 0, len)?;

        // Commit the addition. If was empty, first == last.
        let first_pos = if was_empty { new_last.pos } else { self.first.pos };
        self.write_header(self.file_len, self.elem_cnt + 1, first_pos, new_last.pos)?;
        self.last = new_last;
        self.elem_cnt += 1;

        if was_empty {
            self.first = self.last;
        }

        // dbg!("add", len, self.first, self.last);

        Ok(())
    }

    /// Reads the eldest element. Returns `OK(None)` if the queue is empty.
    pub fn peek(&mut self) -> Result<Option<Box<[u8]>>> {
        if self.is_empty() {
            Ok(None)
        } else {
            let len = self.first.len;
            let mut data = vec![0; len as usize].into_boxed_slice();

            self.ring_read(self.first.pos + Element::HEADER_LENGTH_V2 as u64, &mut data, 0, len)?;

            Ok(Some(data))
        }
    }

    /// Removes the eldest element.
    pub fn remove(&mut self) -> Result<()> {
        self.remove_n(1)
    }

    /// Removes the eldest `n` elements.
    pub fn remove_n(&mut self, n: u32) -> Result<()> {
        if n == 0 || self.is_empty() {
            return Ok(());
        }

        let n = min(n, self.elem_cnt);

        if n == self.elem_cnt {
            return self.clear();
        }

        let erase_start_pos = self.first.pos;
        let mut erase_total_len = 0usize;

        // Read the position and length of the new first element.
        let mut cur_elem = self.first.clone();
        for _ in 0..n {
            erase_total_len += Element::HEADER_LENGTH_V2 + cur_elem.len;
            let next_pos =
                self.wrap_pos(cur_elem.pos + Element::HEADER_LENGTH_V2 as u64 + cur_elem.len as u64);

            cur_elem = self.read_element(next_pos)?;
        }

        // Commit the header.
        self.write_header(self.file_len, self.elem_cnt - n, cur_elem.pos, self.last.pos)?;
        self.elem_cnt -= n;
        self.first = Element::new(cur_elem.pos, cur_elem.len);

        if self.overwrite_on_remove {
            self.ring_erase(erase_start_pos, erase_total_len)?;
        }

        // dbg!("  remove", self.elem_cnt, self.first, self.last);

        Ok(())
    }

    /// Clears this queue. Truncates the file to the initial size.
    pub fn clear(&mut self) -> Result<()> {

        self.elem_cnt = 0;

        if self.overwrite_on_remove {
            self.seek(self.header.hlen)?;
            let len = QueueFile::INITIAL_LENGTH - self.header.hlen;
            self.write(&QueueFile::ZEROES, 0, len as usize)?;
        }

        // Commit the header.
        if self.rawblkdev {
            self.write_header(self.file_len, 0, self.first.pos, self.last.pos)?;
        } else {
            self.write_header(QueueFile::INITIAL_LENGTH, 0, 0, 0)?;
            
            self.first = Element::EMPTY;
            self.last = Element::EMPTY;

            if self.file_len > QueueFile::INITIAL_LENGTH {
                self.sync_set_len(QueueFile::INITIAL_LENGTH)?;
            }
            self.file_len = QueueFile::INITIAL_LENGTH;
        }

        // dbg!("clear", self.elem_cnt, self.first, self.last);
        Ok(())
    }

    /// Returns an iterator over elements in this queue.
    pub fn iter(&mut self) -> Iter<'_> {
        let pos = self.first.pos;
        // dbg!(self.elem_cnt, pos);
        Iter { queue_file: self, next_elem_index: 0, next_elem_pos: pos }
    }

    fn used_bytes(&self) -> u64 {
        if self.elem_cnt == 0 {
            self.header.hlen
        } else if self.last.pos >= self.first.pos {
            // Contiguous queue.
            (self.last.pos - self.first.pos)
                + Element::HEADER_LENGTH_V2 as u64
                + self.last.len as u64
                + self.header.hlen
        } else {
            // tail < head. The queue wraps.
            self.last.pos + Element::HEADER_LENGTH_V2 as u64 + self.last.len as u64 + self.file_len
                - self.first.pos
        }
    }

    fn remaining_bytes(&self) -> u64 {
        self.file_len - self.used_bytes()
    }

    /// Writes header atomically. The arguments contain the updated values. The struct member fields
    /// should not have changed yet. This only updates the state in the file. It's up to the caller
    /// to update the class member variables *after* this call succeeds. Assumes segment writes are
    /// atomic in the underlying file system.
    fn write_header(
        &mut self, file_len: u64, elem_cnt: u32, first_pos: u64, last_pos: u64,
    ) -> io::Result<()> {

        self.header.file_len = file_len;
        self.header.elem_cnt = elem_cnt;
        self.header.head_pos = first_pos;
        self.header.tail_pos = last_pos;
        self.header.calc_bytes();

        self.seek(0)?;
        let sync_writes = self.sync_writes;
        Self::write_to_file(
            &mut self.file,
            sync_writes,
            self.header.hdr_buf.as_ref(),
            0,
            self.header.hlen as usize,
        )
    }

    fn read_element(&mut self, pos: u64) -> Result<Element> {
        if pos == 0 {
            Ok(Element::EMPTY)
        } else {
            
            // read Element header: length and CRC
            let mut buf = [0u8; Element::HEADER_LENGTH_V2];
            self.ring_read(pos, &mut buf, 0, Element::HEADER_LENGTH_V2)?;

            // let mut buf = BytesMut::from(&buf[..]); // bytes 0.5 and above
            let mut bbuf = buf.into_buf();
            let len = bbuf.get_u32_be() as usize;
            let crc = bbuf.get_u32_be();

            if crc != Element::crc(len) {
                return Err(
                    Error::CorruptedFile { msg: format!("Bad element hdr at {}", pos) });
            }

            Ok(Element::new(pos, len))
        }
    }

    /// Wraps the position if it exceeds the end of the file.
    fn wrap_pos(&self, pos: u64) -> u64 {
        if pos < self.file_len { pos } else { self.header.hlen + pos - self.file_len }
    }

    /// Writes `n` bytes from buffer to position in file. Automatically wraps write if position is
    /// past the end of the file or if buffer overlaps it.
    fn ring_write(&mut self, pos: u64, buf: &[u8], off: usize, n: usize) -> io::Result<()> {
        let pos = self.wrap_pos(pos);

        if pos + n as u64 <= self.file_len {
            self.seek(pos)?;
            self.write(buf, off, n)
        } else {
            let before_eof = (self.file_len - pos) as usize;

            self.seek(pos)?;
            self.write(buf, off, before_eof)?;
            self.seek(self.header.hlen)?;
            self.write(buf, off + before_eof, n - before_eof)
        }
    }

    fn ring_erase(&mut self, pos: u64, n: usize) -> io::Result<()> {
        let mut pos = pos;
        let mut len = n as i64;

        while len > 0 {
            let chunk_len = min(len, QueueFile::ZEROES.len() as i64);

            self.ring_write(pos, &QueueFile::ZEROES, 0, chunk_len as usize)?;

            len -= chunk_len;
            pos += chunk_len as u64;
        }

        Ok(())
    }

    /// Reads `n` bytes into buffer from file. Wraps if necessary.
    fn ring_read(&mut self, pos: u64, buf: &mut [u8], off: usize, n: usize) -> io::Result<()> {
        let pos = self.wrap_pos(pos);

        if pos + n as u64 <= self.file_len {
            self.seek(pos)?;
            self.read(buf, off, n)
        } else {
            let before_eof = (self.file_len - pos) as usize;

            self.seek(pos)?;
            self.read(buf, off, before_eof)?;
            self.seek(self.header.hlen)?;
            self.read(buf, off + before_eof, n - before_eof)
        }
    }

    /// If necessary, expands the file to accommodate an additional element of the given length.
    fn expand_if_necessary(&mut self, data_len: usize) -> io::Result<()> {
        let elem_len = Element::HEADER_LENGTH_V2 + data_len;
        let mut rem_bytes = self.remaining_bytes();

        if rem_bytes >= elem_len as u64 {
            return Ok(());
        }

        let mut prev_len = self.file_len;
        let mut new_len = prev_len;

        while rem_bytes < elem_len as u64 {
            rem_bytes += prev_len;
            new_len = prev_len << 1;
            prev_len = new_len;
        }

        // Calculate the position of the tail end of the data in the ring buffer
        let end_of_last_elem =
            self.wrap_pos(self.last.pos + Element::HEADER_LENGTH_V2 as u64 + self.last.len as u64);
        let mut count = 0u64;

        // If the buffer is split, we need to make it contiguous
        if end_of_last_elem <= self.first.pos {
            count = end_of_last_elem - self.header.hlen;

            let write_pos = self.seek(self.file_len)?;
            self.transfer(self.header.hlen, write_pos, count)?;
        }

        // Commit the expansion.
        if self.last.pos < self.first.pos {
            let new_last_pos = self.file_len + self.last.pos - self.header.hlen;
            self.write_header(new_len, self.elem_cnt, self.first.pos, new_last_pos)?;
            self.last = Element::new(new_last_pos, self.last.len);
        } else {
            self.write_header(new_len, self.elem_cnt, self.first.pos, self.last.pos)?;
        }

        self.file_len = new_len;

        if self.overwrite_on_remove {
            self.ring_erase(self.header.hlen, count as usize)?;
        }

        Ok(())
    }
}

// I/O Helpers
impl QueueFile {
    const TRANSFER_BUFFER_SIZE: usize = 128 * 1024;

    fn seek(&mut self, pos: u64) -> io::Result<u64> {
        self.file.seek(SeekFrom::Start(pos))
    }

    fn read(&mut self, buf: &mut [u8], off: usize, n: usize) -> io::Result<()> {
        self.file.read_exact(&mut buf[off..off + n])
    }

    fn write_to_file(
        file: &mut File, sync_writes: bool, buf: &[u8], off: usize, n: usize,
    ) -> io::Result<()> {
        file.write_all(&buf[off..off + n])?;

        if sync_writes { file.sync_data() } else { Ok(()) }
    }

    fn write(&mut self, buf: &[u8], off: usize, n: usize) -> io::Result<()> {
        Self::write_to_file(&mut self.file, self.sync_writes, buf, off, n)
    }

    /// Transfer `count` bytes starting from `read_pos` to `write_pos`.
    fn transfer(&mut self, read_pos: u64, write_pos: u64, count: u64) -> io::Result<()> {
        assert!(read_pos < self.file_len);
        assert!(write_pos <= self.file_len);
        assert!(count < self.file_len);
        assert!(count <= i64::max_value() as u64);

        let mut read_pos = read_pos;
        let mut write_pos = write_pos;
        let mut bytes_left = count as i64;

        while bytes_left > 0 {
            self.file.seek(SeekFrom::Start(read_pos))?;
            let bytes_to_read = min(bytes_left as usize, Self::TRANSFER_BUFFER_SIZE);
            let bytes_read = self.file.read(&mut self.transfer_buf[..bytes_to_read])?;

            self.file.seek(SeekFrom::Start(write_pos))?;
            self.file.write_all(&self.transfer_buf[..bytes_read])?;

            read_pos += bytes_read as u64;
            write_pos += bytes_read as u64;
            bytes_left -= bytes_read as i64;
        }

        // Should we `sync_data()` in internal loop instead?
        if self.sync_writes {
            self.file.sync_data()?;
        }

        Ok(())
    }

    fn sync_set_len(&mut self, new_len: u64) -> io::Result<()> {
        self.file.set_len(new_len)?;
        self.file.sync_all()?;
        Ok(())
    }
}


/// Queue-file Main Header
/// Versioned Header (32 bytes):
///   1 bit            Versioned indicator [0 = legacy, 1 = versioned]
///   31 bits          Version
///   8 bytes          File length
///   4 bytes          Element count
///   8 bytes          Head element position
///   8 bytes          Tail element position
///   4 bytes          CRC
///
/// hdr_len: version based size
#[derive(Debug)]
struct Header 
{
    flags: u16,    // bit 0x80_00 should always be set
    version:  u16,
    file_len: u64,
    elem_cnt: u32,
    head_pos: u64, // position from front of file (caution other pos are from after header)
    tail_pos: u64, // position from front of file (caution other pos are from after header)
    crc: u32,
    hdr_buf: BytesMut,
    hlen: u64,
}

impl Header {
    const FLAG_DEFAULT: u16 = 0x80_00;
    const FLAG_RAWBLKDEV: u16 = 0x00_01;

    fn new(ver: u16) -> Header {
        let hlen = Self::version_len(ver);

        let mut hdr = Header {
            flags: Self::FLAG_DEFAULT,
            version: ver,
            file_len: 0,
            elem_cnt: 0,
            head_pos: 0,
            tail_pos: 0,
            crc: 0,
            hdr_buf: BytesMut::with_capacity(hlen as usize),
            hlen,
        };
        hdr.crc = hdr.calc_crc();
        hdr
    }

    fn version_len(ver: u16) -> u64 {
        match ver {
            0 => 28,
            1 => 32,
            2 => 36,
            _ => 36,
        }
    }

    fn set_rawblkdev(&mut self, tf: bool) {
        self.flags = Self::FLAG_DEFAULT;
        if tf {
            self.flags |= Self::FLAG_RAWBLKDEV;
        }
    }

    fn is_rawblkdev(&self) -> bool {
        self.flags & Self::FLAG_RAWBLKDEV != 0
    }

    /// calc_bytes takes header values and packs them into hdr_buf
    fn calc_bytes(&mut self) {
        self.hdr_buf.clear();

        let version = self.version;
        if version >= 1 {
            // Never allow write values that will render file unreadable by Java library.
            assert!(self.file_len <= i64::max_value() as u64);
            assert!(self.elem_cnt <= i32::max_value() as u32);
            assert!(self.head_pos <= i64::max_value() as u64);
            assert!(self.tail_pos <= i64::max_value() as u64);

            self.hdr_buf.put_u16_be(self.flags);
            self.hdr_buf.put_u16_be(self.version);
            self.hdr_buf.put_u64_be(self.file_len);
            self.hdr_buf.put_u32_be(self.elem_cnt);
            self.hdr_buf.put_u64_be(self.head_pos);
            self.hdr_buf.put_u64_be(self.tail_pos);

            if version > 1 {
                let mut digest = crc32::Digest::new(crc32::IEEE);
                digest.write(&self.hdr_buf);
                let crc = digest.sum32();            
                self.hdr_buf.put_u32_be(crc);
            }

        } else {
            // Never allow write values that will render file unreadable by Java library.
            assert!(self.file_len <= i32::max_value() as u64);
            assert!(self.elem_cnt <= i32::max_value() as u32);
            assert!(self.head_pos <= i32::max_value() as u64);
            assert!(self.tail_pos <= i32::max_value() as u64);

            self.hdr_buf.put_u64_be(self.file_len);
            self.hdr_buf.put_i32_be(self.elem_cnt as i32);
            self.hdr_buf.put_u64_be(self.head_pos);
            self.hdr_buf.put_u64_be(self.tail_pos);
        }
        // self.hdr_buf        
    }

    fn from_bytes(buf: &[u8]) -> Header {
        let is_version0 = (buf[0] & 0x80) == 0;

        // let mut cbuf = BytesMut::from(&buf[..]); // bytes 0.5 and above
        let mut cbuf = std::io::Cursor::new(&buf);
        let mut flags = Self::FLAG_DEFAULT;
        let version = if is_version0 {
            0 
        } else {
            flags = cbuf.get_u16_be();
            cbuf.get_u16_be()
        };

        let mut hdr = Header::new(version);
        hdr.flags    = flags;
        hdr.file_len = cbuf.get_u64_be();
        hdr.elem_cnt = cbuf.get_u32_be();
        hdr.head_pos = cbuf.get_u64_be();
        hdr.tail_pos = cbuf.get_u64_be();

        if version > 1 {
            hdr.crc = cbuf.get_u32_be();
        }
        hdr
    }

    fn verify_crc(&self) -> bool {
        let crc = self.calc_crc();
        crc == self.crc
    }

    fn calc_crc(&self) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);

        digest.write(&self.flags.to_be_bytes());
        digest.write(&self.version.to_be_bytes());
        digest.write(&self.file_len.to_be_bytes());
        digest.write(&self.elem_cnt.to_be_bytes());
        digest.write(&self.head_pos.to_be_bytes());
        digest.write(&self.tail_pos.to_be_bytes());

        digest.sum32()
    }   
}

#[derive(Copy, Clone, Debug)]
struct Element {
    pos: u64,
    len: usize,
}

impl Element {
    const EMPTY: Element = Element { pos: 0, len: 0 };
    const HEADER_LENGTH_V2: usize = 8;

    fn new(pos: u64, len: usize) -> Self {
        assert!(
            pos <= i64::max_value() as u64,
            "element position must be less than {}",
            i64::max_value()
        );
        assert!(
            len <= i32::max_value() as usize,
            "element length must be less than {}",
            i32::max_value()
        );

        Element { pos, len }
    }

    fn crc(len: usize) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&len.to_be_bytes());
        digest.sum32()
    }
}

pub struct Iter<'a> {
    queue_file: &'a mut QueueFile,
    next_elem_index: usize,
    next_elem_pos: u64,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.queue_file.is_empty() || self.next_elem_index >= self.queue_file.elem_cnt as usize {
            return None;
        }

        let current = self.queue_file.read_element(self.next_elem_pos).ok()?;
        self.next_elem_pos = self.queue_file.wrap_pos(current.pos + Element::HEADER_LENGTH_V2 as u64);

        let mut data = vec![0; current.len].into_boxed_slice();
        self.queue_file.ring_read(self.next_elem_pos, &mut data, 0, current.len).ok()?;

        self.next_elem_pos = self
            .queue_file
            .wrap_pos(current.pos + Element::HEADER_LENGTH_V2 as u64 + current.len as u64);
        self.next_elem_index += 1;

        // dbg!(self.next_elem_pos, self.next_elem_index);

        Some(data)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let elems_left = self.queue_file.elem_cnt as usize - self.next_elem_index;

        (elems_left, Some(elems_left))
    }
}


#[cfg(test)]
mod test_rawblockdev;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs::remove_file;
    use std::iter;
    use std::path::PathBuf;

    #[cfg(test)]
    use pretty_assertions::assert_eq;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use super::*;

    pub fn gen_rand_data(size: usize) -> Box<[u8]> {
        let mut buf = vec![0u8; size];
        thread_rng().fill(buf.as_mut_slice());

        buf.into_boxed_slice()
    }

    pub fn gen_rand_file_name() -> String {
        let mut rng = thread_rng();
        let mut file_name =
            iter::repeat(()).map(|()| rng.sample(Alphanumeric)).take(16).collect::<String>();

        file_name.push_str(".qf");

        file_name
    }

    fn new_queue_file(overwrite_on_remove: bool) -> (PathBuf, QueueFile) {
        new_queue_file_ex(overwrite_on_remove, false)
    }

    fn new_legacy_queue_file(overwrite_on_remove: bool) -> (PathBuf, QueueFile) {
        new_queue_file_ex(overwrite_on_remove, true)
    }

    fn new_queue_file_ex(overwrite_on_remove: bool, force_legacy: bool) -> (PathBuf, QueueFile) {
        let file_name = gen_rand_file_name();
        let path = Path::new(file_name.as_str());
        let mut queue_file = if force_legacy {
            QueueFile::open_legacy(path).unwrap()
        } else {
            QueueFile::open(path).unwrap()
        };

        queue_file.set_overwrite_on_remove(overwrite_on_remove);

        (path.to_owned(), queue_file)
    }

    const ITERATIONS: usize = 100;
    const MIN_N: usize = 1;
    const MAX_N: usize = 10;
    const MIN_DATA_SIZE: usize = 0;
    const MAX_DATA_SIZE: usize = 4096;
    const CLEAR_PROB: f64 = 0.05;
    const REOPEN_PROB: f64 = 0.01;

    #[test]
    fn test_queue_file_iter() {
        let (path, mut qf) = new_queue_file(false);
        let mut q: VecDeque<Box<[u8]>> = VecDeque::with_capacity(128);

        add_rand_n_elems(&mut q, &mut qf, 10, 15, MIN_DATA_SIZE, MAX_DATA_SIZE);

        for elem in qf.iter() {
            assert_eq!(elem, q.pop_front().unwrap());
        }
        assert_eq!(q.is_empty(), true);

        let qv = qf.iter().collect::<Vec<_>>();
        assert_eq!(qv.len(), qf.size());

        for e in qf.iter().zip(qv.iter()) {
            assert_eq!(&e.0, e.1);
        }

        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_queue_file() {
        let (path, qf) = new_queue_file(false);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_queue_file_with_zero() {
        let (path, qf) = new_queue_file(true);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_legacy_queue_file() {
        let (path, qf) = new_legacy_queue_file(false);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_legacy_queue_file_with_zero() {
        let (path, qf) = new_legacy_queue_file(true);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    fn add_rand_n_elems(
        q: &mut VecDeque<Box<[u8]>>, qf: &mut QueueFile, min_n: usize, max_n: usize,
        min_data_size: usize, max_data_size: usize,
    ) -> usize
    {
        let mut rng = thread_rng();

        let n = rng.gen_range(min_n, max_n);

        for _ in 0..n {
            let data_size = rng.gen_range(min_data_size, max_data_size);
            let data = gen_rand_data(data_size);

            assert_eq!(qf.add(data.as_ref()).is_ok(), true);
            q.push_back(data);
        }

        n
    }

    fn verify_rand_n_elems(
        q: &mut VecDeque<Box<[u8]>>, qf: &mut QueueFile, min_n: usize, max_n: usize,
    ) -> usize {
        if qf.is_empty() {
            return 0;
        }

        let n = if qf.size() == 1 { 1 } else { thread_rng().gen_range(min_n, max_n) };

        for _ in 0..n {
            let d0 = q.pop_front().unwrap();
            let d1 = qf.peek().unwrap().unwrap();
            assert_eq!(qf.remove().is_ok(), true);

            assert_eq!(d0, d1);
        }

        n
    }

    pub fn simulate_use(
        path: &Path, mut qf: QueueFile, iters: usize, min_n: usize, max_n: usize,
        min_data_size: usize, max_data_size: usize, clear_prob: f64, reopen_prob: f64,
    )
    {
        let mut q: VecDeque<Box<[u8]>> = VecDeque::with_capacity(128);

        add_rand_n_elems(&mut q, &mut qf, min_n, max_n, min_data_size, max_data_size);

        for _ in 0..iters {
            assert_eq!(q.len(), qf.size());

            if thread_rng().gen_bool(reopen_prob) {
                let overwrite_on_remove = qf.get_overwrite_on_remove();

                drop(qf);
                qf = QueueFile::open(path).unwrap();
                qf.set_overwrite_on_remove(overwrite_on_remove);
            }
            if thread_rng().gen_bool(clear_prob) {
                q.clear();
                qf.clear().unwrap()
            }

            let elem_cnt = qf.size();
            verify_rand_n_elems(&mut q, &mut qf, 1, elem_cnt);
            add_rand_n_elems(&mut q, &mut qf, min_n, max_n, min_data_size, max_data_size);
        }

        while let Ok(Some(data)) = qf.peek() {
            assert_eq!(qf.remove().is_ok(), true);
            assert_eq!(data, q.pop_front().unwrap());
        }

        assert_eq!(q.is_empty(), true);
        assert_eq!(qf.is_empty(), true);
    }
}
