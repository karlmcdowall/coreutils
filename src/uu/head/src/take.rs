// This file is part of the uutils coreutils package.
//
// For the full copyright and license information, please view the LICENSE
// file that was distributed with this source code.
//! Take all but the last elements of an iterator.
use std::io::ErrorKind;
use std::io::Read;

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
    fn fill_buffer<R>(&mut self, reader: &mut R) -> std::io::Result<usize>
    where
        R: Read,
    {
        self.buffer.resize(Self::buffer_size(), 0);
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

    fn remaining_bytes(&self) -> usize {
        self.buffer.len() - self.start_index
    }

    const fn buffer_size() -> usize {
        BUF_SIZE
    }

    fn consume(&mut self, n: usize) -> &[u8] {
        let end_index = n + self.start_index;
        assert!(end_index <= self.buffer.len());
        let slice = &self.buffer[self.start_index..end_index];
        self.start_index = end_index;
        slice
    }
}

pub fn take_all_but2<R: Read>(reader: R, n: usize) -> TakeAllBut2<R> {
    TakeAllBut2::new(reader, n)
}

pub struct TakeAllBut2<R>
where
    R: Read,
{
    // Todo - rename to inner
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
}

impl<R: Read> Read for TakeAllBut2<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Try to buffer at least buf.len() + n bytes so we can fill the client buffer.
        let target_minimum_bytes = buf.len() + self.n;
        while self.buffered_bytes < target_minimum_bytes {
            let mut new_buffer = self.empty_buffers.pop().unwrap_or_else(TakeAllBuffer::new);
            let filled_bytes = new_buffer.fill_buffer(&mut self.inner)?;
            self.buffers.push_back(new_buffer);
            self.buffered_bytes += filled_bytes;
            // Todo - add a method onto TakeAllBuffer for this...
            if filled_bytes != TakeAllBuffer::buffer_size() {
                // If we only managed a partial fill then we must be EOF -> break.
                break;
            }
        }

        // Now copy as many bytes as we can into buf.
        let mut bytes_coppied = 0;
        while bytes_coppied < buf.len() {
            // If we've got <= n bytes buffered we must be done - break.
            if self.buffered_bytes <= self.n {
                break;
            }
            // Limit the number of bytes we want to copy so we don't drop bellow n-bytes buffered.
            let max_bytes_to_copy = self.buffered_bytes - self.n;
            assert!(max_bytes_to_copy > 0);
            let bytes_remaining_to_copy = (buf.len() - bytes_coppied).min(max_bytes_to_copy);
            let front_buffer = &mut self.buffers.front_mut().unwrap();

            let bytes_to_copy_from_front_buffer =
                front_buffer.remaining_bytes().min(bytes_remaining_to_copy);
            let buffer_to_copy = front_buffer.consume(bytes_to_copy_from_front_buffer);
            let target_slice =
                &mut buf[bytes_coppied..(bytes_coppied + bytes_to_copy_from_front_buffer)];
            target_slice.copy_from_slice(buffer_to_copy);
            bytes_coppied += bytes_to_copy_from_front_buffer;
            self.buffered_bytes -= bytes_coppied;
            if front_buffer.remaining_bytes() == 0 {
                self.empty_buffers.push(self.buffers.pop_front().unwrap());
            }
        }

        Ok(bytes_coppied)
    }
}

struct TakeAllLinesBuffer {
    buffer: Vec<u8>,
    valid_bytes: usize,
    start_index: usize,
    separator: u8,
    lines: usize,
}

// Todo - rename TakeAllButLinesBuffer
impl TakeAllLinesBuffer {
    fn new(separator: u8) -> Self {
        let mut instance = TakeAllLinesBuffer {
            buffer: vec![],
            // Todo - use Vec::truncate and remove valid_bytes.
            valid_bytes: 0,
            start_index: 0,
            separator,
            lines: 0,
        };
        instance.buffer.resize(Self::buffer_size(), 0);
        instance
    }
    fn fill_buffer<R>(&mut self, reader: &mut R) -> std::io::Result<usize>
    where
        R: Read,
    {
        self.valid_bytes = 0;
        self.start_index = 0;
        self.lines = 0;
        loop {
            let read_result = reader.read(&mut self.buffer[self.valid_bytes..]);
            match read_result {
                Ok(0) => break, // EoF
                Ok(n) => self.valid_bytes += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
            if self.valid_bytes == self.buffer.len() {
                break;
            }
        }
        // Now count the number of lines...
        for _i in memchr_iter(self.separator, &self.buffer[..]) {
            self.lines += 1;
        }
        Ok(self.valid_bytes)
    }

    fn valid_bytes(&self) -> usize {
        self.valid_bytes - self.start_index
    }

    const fn buffer_size() -> usize {
        BUF_SIZE
    }

    fn consume(&mut self, n: usize) -> &[u8] {
        let end_index = n + self.start_index;
        assert!(end_index <= self.valid_bytes);
        let slice = &self.buffer[self.start_index..end_index];
        self.start_index = end_index;
        slice
    }

    fn remaining_bytes(&self) -> usize {
        self.valid_bytes - self.start_index
    }
    fn next_offset(&self) -> Option<usize> {
        memchr(
            self.separator,
            &self.buffer[self.start_index..self.valid_bytes],
        )
        .map(|n| n + 1)
    }

    fn lines(&self) -> usize {
        self.lines
    }
}

pub fn take_all_but_lines<R: Read>(reader: R, n: usize, separator: u8) -> TakeAllButLines<R> {
    TakeAllButLines::new(reader, n, separator)
}

pub struct TakeAllButLines<R>
where
    R: Read,
{
    inner: R,
    n: usize,
    buffers: VecDeque<TakeAllLinesBuffer>,
    empty_buffers: Vec<TakeAllLinesBuffer>,
    buffered_lines: usize,
    separator: u8,
}

impl<R: Read> TakeAllButLines<R> {
    fn new(reader: R, n: usize, separator: u8) -> Self {
        assert!(n > 0);
        TakeAllButLines {
            inner: reader,
            n,
            buffers: VecDeque::new(),
            empty_buffers: vec![],
            buffered_lines: 0,
            separator,
        }
    }
}

impl<R: Read> Read for TakeAllButLines<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Buffer at least n lines.
        while self.buffered_lines <= self.n {
            let mut new_buffer = self
                .empty_buffers
                .pop()
                .unwrap_or_else(|| TakeAllLinesBuffer::new(self.separator));
            let bytes_read = new_buffer.fill_buffer(&mut self.inner)?;
            if bytes_read == 0 {
                break;
            }
            self.buffered_lines += new_buffer.lines();
            //             eprintln!("Read {} new lines", new_buffer.lines());
            self.buffers.push_back(new_buffer);
        }

        //        eprintln!("About to start writing. Buffered_lines = {}", self.buffered_lines);
        //        eprintln!("Bufffer count = {}", self.buffers.len());
        let mut bytes_coppied = 0;
        while self.buffered_lines > self.n {
            //            eprintln!("About to start writing. Buffered_lines = {}", self.buffered_lines);
            //            eprintln!("Bufffer count = {}", self.buffers.len());

            // Copy as many lines into buf as we can fit, without dropping below our
            // minimum number of buffered lines.
            let front_buffer = &mut self.buffers.front_mut().unwrap();

            let next_string_offset = front_buffer.next_offset();
            let bytes_remaining_in_buf = buf.len() - bytes_coppied;
            // This is naughty that we don't decrement front_buffer.lines.
            let bytes_to_consume = next_string_offset
                .unwrap_or_else(|| front_buffer.remaining_bytes())
                .min(bytes_remaining_in_buf);
            if next_string_offset.is_some_and(|val| val == bytes_to_consume) {
                // We're consuming a whole line. Decrement our count.
                self.buffered_lines -= 1;
            }
            let buffer_to_copy = front_buffer.consume(bytes_to_consume);
            let target_slice = &mut buf[bytes_coppied..(bytes_coppied + bytes_to_consume)];
            target_slice.copy_from_slice(buffer_to_copy);
            bytes_coppied += bytes_to_consume;
            //            eprintln!("bytes_to_consume: {}, bytes_coppied: {}", bytes_to_consume, bytes_coppied);
            if front_buffer.valid_bytes() == 0 {
                self.empty_buffers.push(self.buffers.pop_front().unwrap());
            }
        }
        //        eprintln!("Coppied {}", bytes_coppied);
        Ok(bytes_coppied)
        // Try to buffer at least buf.len() + n bytes so we can fill the client buffer.
        // let target_minimum_bytes = buf.len() + self.n;
        // while self.buffered_bytes < target_minimum_bytes {
        //     let mut new_buffer = self.empty_buffers.pop().unwrap_or_else(TakeAllBuffer::new);
        //     let filled_bytes = new_buffer.fill_buffer(&mut self.)?;
        //     self.buffers.push_back(new_buffer);
        //     self.buffered_bytes += filled_bytes;
        //     // Todo - add a method onto TakeAllBuffer for this...
        //     if filled_bytes != TakeAllBuffer::buffer_size() {
        //         // If we only managed a partial fill then we must be EOF -> break.
        //         break;
        //     }
        // }

        // Now copy as many bytes as we can into buf.
        // let mut bytes_coppied = 0;
        // while bytes_coppied < buf.len() {
        //     // If we've got <= n bytes buffered we must be done - break.
        //     if self.buffered_bytes <= self.n {
        //         break;
        //     }
        //     // Limit the number of bytes we want to copy so we don't drop bellow n-bytes buffered.
        //     let max_bytes_to_copy = self.buffered_bytes - self.n;
        //     assert!(max_bytes_to_copy > 0);
        //     let bytes_remaining_to_copy = (buf.len() - bytes_coppied).min(max_bytes_to_copy);
        //     let front_buffer = &mut self.buffers.front_mut().unwrap();

        //     let bytes_to_copy_from_front_buffer =
        //         front_buffer.valid_bytes().min(bytes_remaining_to_copy);
        //     let buffer_to_copy = front_buffer.consume(bytes_to_copy_from_front_buffer);
        //     let target_slice =
        //         &mut buf[bytes_coppied..(bytes_coppied + bytes_to_copy_from_front_buffer)];
        //     target_slice.copy_from_slice(buffer_to_copy);
        //     bytes_coppied += bytes_to_copy_from_front_buffer;
        //     self.buffered_bytes -= bytes_coppied;
        //     if front_buffer.valid_bytes() == 0 {
        //         self.empty_buffers.push(self.buffers.pop_front().unwrap());
        //     }
        // }

        // Ok(bytes_coppied)
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
