// This file is part of the uutils coreutils package.
//
// For the full copyright and license information, please view the LICENSE
// file that was distributed with this source code.
//! Take all but the last elements of an iterator.
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;

use std::collections::VecDeque;

use memchr::{memchr, memchr_iter};

//use uucore::ringbuffer::RingBuffer;

const BUF_SIZE: usize = 65536;

struct TakeAllBuffer {
    buffer: Vec<u8>,
    start_index: usize,
}

impl TakeAllBuffer {
    fn new() -> Self {
        TakeAllBuffer {
            buffer: vec![],
            start_index: 0,
        }
    }
    fn fill_buffer(&mut self, reader: &mut impl Read) -> std::io::Result<usize> {
        self.buffer.resize(Self::max_buffer_size(), 0);
        let mut valid_bytes = 0;
        self.start_index = 0;
        loop {
            let read_result = reader.read(&mut self.buffer[valid_bytes..]);
            match read_result {
                Ok(0) => break, // EoF
                Ok(n) => valid_bytes += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
            if valid_bytes == self.buffer.len() {
                break;
            }
        }
        self.buffer.truncate(valid_bytes);
        Ok(valid_bytes)
    }

    fn write(&mut self, writer: &mut impl Write, max_bytes: usize) -> std::io::Result<usize> {
        let bytes_to_write = self.remaining_bytes().min(max_bytes);
        assert!(bytes_to_write > 0);
        let end_index = self.start_index + bytes_to_write;
        writer.write_all(&self.buffer[self.start_index..end_index])?;
        self.start_index = end_index;
        Ok(bytes_to_write)
    }

    fn remaining_bytes(&self) -> usize {
        self.buffer.len() - self.start_index
    }

    fn is_empty(&self) -> bool {
        self.remaining_bytes() == 0
    }

    const fn max_buffer_size() -> usize {
        BUF_SIZE
    }
}

pub fn take_all_but2<R: Read>(reader: R, n: usize) -> TakeAllBut2<R> {
    TakeAllBut2::new(reader, n)
}

pub struct TakeAllBut2<R>
where
    R: Read,
{
    inner: R,
    n: usize,
    buffers: VecDeque<TakeAllBuffer>,
    empty_buffers: Vec<TakeAllBuffer>,
    buffered_bytes: usize,
}

impl<R: Read> TakeAllBut2<R> {
    fn new(reader: R, n: usize) -> Self {
        TakeAllBut2 {
            inner: reader,
            n,
            buffers: VecDeque::new(),
            empty_buffers: vec![],
            buffered_bytes: 0,
        }
    }
    pub fn write(&mut self, writer: &mut impl Write) -> std::io::Result<usize> {
        let mut bytes_coppied = 0;
        loop {
            // Try to buffer at least a full buffer of extra data.
            let target_minimum_buffered_bytes = TakeAllBuffer::max_buffer_size() + self.n;
            while self.buffered_bytes < target_minimum_buffered_bytes {
                let mut new_buffer = self.empty_buffers.pop().unwrap_or_else(TakeAllBuffer::new);
                let filled_bytes = new_buffer.fill_buffer(&mut self.inner)?;
                self.buffers.push_back(new_buffer);
                self.buffered_bytes += filled_bytes;
                // Todo - add a method onto TakeAllBuffer for this...
                if filled_bytes != TakeAllBuffer::max_buffer_size() {
                    // If we only managed a partial fill then we must be EOF -> break.
                    break;
                }
            }

            // If we've got <=n bytes buffered here we're done.
            if self.buffered_bytes <= self.n {
                break;
            }

            // Since we have some data buffered, can assume we have 1 bufffer.
            let mut front_buffer = self.buffers.pop_front().unwrap();
            let excess_buffered_bytes = self.buffered_bytes - self.n;
            let bytes_written = front_buffer.write(writer, excess_buffered_bytes)?;
            self.buffered_bytes -= bytes_written;
            bytes_coppied += bytes_written;
            // If the front buffer is empty (which it probably is), push it into the empty-buffer-pool.
            if front_buffer.is_empty() {
                self.empty_buffers.push(front_buffer);
            } else {
                self.buffers.push_front(front_buffer);
            }
        }
        Ok(bytes_coppied)
    }
}

struct TakeAllLinesBuffer {
    buffer: Vec<u8>,
    start_index: usize,
    lines: usize,
    separator: u8,
}

impl TakeAllLinesBuffer {
    fn new(separator: u8) -> Self {
        TakeAllLinesBuffer {
            buffer: vec![],
            start_index: 0,
            lines: 0,
            separator
        }
    }
    fn fill_buffer(&mut self, reader: &mut impl Read) -> std::io::Result<usize> {
        self.buffer.resize(Self::max_buffer_size(), 0);
        let mut valid_bytes = 0;
        self.start_index = 0;
        loop {
            let read_result = reader.read(&mut self.buffer[valid_bytes..]);
            match read_result {
                Ok(0) => break, // EoF
                Ok(n) => valid_bytes += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
            if valid_bytes == self.buffer.len() {
                break;
            }
        }
        self.buffer.truncate(valid_bytes);
        // Count the number of lines...
        self.lines = memchr_iter(self.separator, &self.buffer[..]).count();
        Ok(valid_bytes)
    }

    fn write(&mut self, writer: &mut impl Write, max_lines: usize) -> std::io::Result<usize> {
        if max_lines <= self.lines {
            // Write everything
            writer.write_all(&self.buffer[self.start_index..])?;
            let ret = Ok(self.lines);
            self.start_index = self.buffer.len();
            self.lines = 0;
            return ret;
        }
        return Ok(0);

        // let bytes_to_write = self.remaining_bytes().min(max_bytes);
        // assert!(bytes_to_write > 0);
        // let end_index = self.start_index + bytes_to_write;
        // writer.write_all(&self.buffer[self.start_index..end_index])?;
        // self.start_index = end_index;
        // Ok(bytes_to_write)
    }

    fn remaining_bytes(&self) -> usize {
        self.buffer.len() - self.start_index
    }

    fn is_empty(&self) -> bool {
        self.remaining_bytes() == 0
    }

    fn lines(&self) -> usize {
        self.lines
    }

    const fn max_buffer_size() -> usize {
        BUF_SIZE
    }
}

pub fn take_all_but_lines<R: Read>(reader: R, n: usize, separator: u8) -> TakeAllLinesBut<R> {
    TakeAllLinesBut::new(reader, n, separator)
}

pub struct TakeAllLinesBut<R>
where
    R: Read,
{
    inner: R,
    n: usize,
    separator: u8,
    buffers: VecDeque<TakeAllLinesBuffer>,
    empty_buffers: Vec<TakeAllLinesBuffer>,
    buffered_lines: usize,
}

impl<R: Read> TakeAllLinesBut<R> {
    fn new(reader: R, n: usize, separator: u8) -> Self {
        TakeAllLinesBut {
            inner: reader,
            n,
            separator,
            buffers: VecDeque::new(),
            empty_buffers: vec![],
            buffered_lines: 0,
        }
    }

    pub fn write(&mut self, writer: &mut impl Write) -> std::io::Result<usize> {
        let mut bytes_coppied = 0;
        loop {
            // Try to buffer enough such that we can write out the entire first buffer.

            while !self.buffers.is_empty() && self.n + self.buffers.front().unwrap().lines() <= self.buffered_lines {
                let mut new_buffer = self.empty_buffers.pop().unwrap_or_else(||TakeAllLinesBuffer::new(self.separator));
                let filled_bytes = new_buffer.fill_buffer(&mut self.inner)?;
                self.buffered_lines += new_buffer.lines();
                self.buffers.push_back(new_buffer);
                // Todo - add a method onto TakeAllLinesBuffer for this...
                if filled_bytes != TakeAllLinesBuffer::max_buffer_size() {
                    // If we only managed a partial fill then we must be EOF -> break.
                    break;
                }
            }

            // If we've got <n bytes buffered here we're done.
            // If we have n lines we might have a bit in the front buffer...
            if self.buffered_lines < self.n {
                break;
            }

            // // Since we have some data buffered, can assume we have 1 bufffer.
            // let mut front_buffer = self.buffers.pop_front().unwrap();
            // let excess_buffered_bytes = self.buffered_bytes - self.n;
            // let bytes_written = front_buffer.write(writer, excess_buffered_bytes)?;
            // self.buffered_bytes -= bytes_written;
            // bytes_coppied += bytes_written;
            // // If the front buffer is empty (which it probably is), push it into the empty-buffer-pool.
            // if front_buffer.is_empty() {
            //     self.empty_buffers.push(front_buffer);
            // } else {
            //     self.buffers.push_front(front_buffer);
            // }
        }
        Ok(bytes_coppied)
    }
}


/// Like `std::io::Take`, but for lines instead of bytes.
///
/// This struct is generally created by calling [`take_lines`] on a
/// reader. Please see the documentation of [`take_lines`] for more
/// details.
pub struct TakeLines<T> {
    inner: T,
    limit: u64,
    separator: u8,
}

impl<T: Read> Read for TakeLines<T> {
    /// Read bytes from a buffer up to the requested number of lines.
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.limit == 0 {
            return Ok(0);
        }
        match self.inner.read(buf) {
            Ok(0) => Ok(0),
            Ok(n) => {
                for i in memchr_iter(self.separator, &buf[..n]) {
                    self.limit -= 1;
                    if self.limit == 0 {
                        return Ok(i + 1);
                    }
                }
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }
}

/// Create an adaptor that will read at most `limit` lines from a given reader.
///
/// This function returns a new instance of `Read` that will read at
/// most `limit` lines, after which it will always return EOF
/// (`Ok(0)`).
///
/// The `separator` defines the character to interpret as the line
/// ending. For the usual notion of "line", set this to `b'\n'`.
pub fn take_lines<R>(reader: R, limit: u64, separator: u8) -> TakeLines<R> {
    TakeLines {
        inner: reader,
        limit,
        separator,
    }
}

#[cfg(test)]
mod tests {

    use std::io::BufRead;
    use std::io::BufReader;

    use crate::take::take_all_but;
    use crate::take::take_lines;

    #[test]
    fn test_fewer_elements() {
        let mut iter = take_all_but([0, 1, 2].iter(), 2);
        assert_eq!(Some(&0), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_same_number_of_elements() {
        let mut iter = take_all_but([0, 1].iter(), 2);
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_more_elements() {
        let mut iter = take_all_but([0].iter(), 2);
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_zero_elements() {
        let mut iter = take_all_but([0, 1, 2].iter(), 0);
        assert_eq!(Some(&0), iter.next());
        assert_eq!(Some(&1), iter.next());
        assert_eq!(Some(&2), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_zero_lines() {
        let input_reader = std::io::Cursor::new("a\nb\nc\n");
        let output_reader = BufReader::new(take_lines(input_reader, 0, b'\n'));
        let mut iter = output_reader.lines().map(|l| l.unwrap());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_fewer_lines() {
        let input_reader = std::io::Cursor::new("a\nb\nc\n");
        let output_reader = BufReader::new(take_lines(input_reader, 2, b'\n'));
        let mut iter = output_reader.lines().map(|l| l.unwrap());
        assert_eq!(Some(String::from("a")), iter.next());
        assert_eq!(Some(String::from("b")), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_more_lines() {
        let input_reader = std::io::Cursor::new("a\nb\nc\n");
        let output_reader = BufReader::new(take_lines(input_reader, 4, b'\n'));
        let mut iter = output_reader.lines().map(|l| l.unwrap());
        assert_eq!(Some(String::from("a")), iter.next());
        assert_eq!(Some(String::from("b")), iter.next());
        assert_eq!(Some(String::from("c")), iter.next());
        assert_eq!(None, iter.next());
    }
}
