// This file is part of the uutils coreutils package.
//
// For the full copyright and license information, please view the LICENSE
// file that was distributed with this source code.
//! Take all but the last elements of an iterator.
use memchr::memchr_iter;
use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};

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
        self.buffer.resize(BUF_SIZE, 0);
        self.start_index = 0;
        loop {
            match reader.read(&mut self.buffer[..]) {
                Ok(n) => {
                    self.buffer.truncate(n);
                    return Ok(n);
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn write_bytes_exact(&mut self, writer: &mut impl Write, bytes: usize) -> std::io::Result<()> {
        let end_index = self.start_index + bytes;
        let buffer_to_write = &self.buffer[self.start_index..end_index];
        self.start_index = end_index;
        writer.write_all(buffer_to_write)
    }

    fn write_all(&mut self, writer: &mut impl Write) -> std::io::Result<usize> {
        let remaining_bytes = self.remaining_bytes();
        self.write_bytes_exact(writer, remaining_bytes)?;
        Ok(remaining_bytes)
    }

    fn write_bytes(&mut self, writer: &mut impl Write, max_bytes: usize) -> std::io::Result<usize> {
        let bytes_to_write = self.remaining_bytes().min(max_bytes);
        self.write_bytes_exact(writer, bytes_to_write)?;
        Ok(bytes_to_write)
    }

    fn remaining_buffer(&self) -> &[u8] {
        &self.buffer[self.start_index..]
    }

    fn remaining_bytes(&self) -> usize {
        self.remaining_buffer().len()
    }

    fn is_empty(&self) -> bool {
        self.remaining_bytes() == 0
    }
}

pub fn copy_all_but_bytes(
    reader: &mut impl Read,
    writer: &mut impl Write,
    n: usize,
) -> std::io::Result<usize> {
    let mut buffers: VecDeque<TakeAllBuffer> = VecDeque::new();
    let mut empty_buffer_pool: Vec<TakeAllBuffer> = vec![];
    let mut buffered_bytes: usize = 0;
    let mut total_bytes_coppied = 0;
    loop {
        loop {
            // Try to buffer at least enough to write the entire first buffer.
            let front_buffer = buffers.front();
            if let Some(front_buffer) = front_buffer {
                if buffered_bytes >= n + front_buffer.remaining_bytes() {
                    break;
                }
            }
            let mut new_buffer = empty_buffer_pool.pop().unwrap_or_else(TakeAllBuffer::new);
            let filled_bytes = new_buffer.fill_buffer(reader)?;
            if filled_bytes == 0 {
                // filled_bytes==0 => Eof
                break;
            }
            buffers.push_back(new_buffer);
            buffered_bytes += filled_bytes;
        }

        // If we've got <=n bytes buffered here we have nothing let to do.
        if buffered_bytes <= n {
            break;
        }

        let excess_buffered_bytes = buffered_bytes - n;
        // Since we have some data buffered, can assume we have >=1 buffer - i.e. safe to unwrap.
        let front_buffer = buffers.front_mut().unwrap();
        let bytes_written = front_buffer.write_bytes(writer, excess_buffered_bytes)?;
        buffered_bytes -= bytes_written;
        total_bytes_coppied += bytes_written;
        // If the front buffer is empty (which it probably is), push it into the empty-buffer-pool.
        if front_buffer.is_empty() {
            empty_buffer_pool.push(buffers.pop_front().unwrap());
        }
    }
    Ok(total_bytes_coppied)
}

struct TakeAllLinesBuffer {
    // Todo - rename buffer -> inner
    buffer: TakeAllBuffer,
    lines: usize,
}

struct BytesAndLines {
    bytes: usize,
    lines: usize,
}

impl TakeAllLinesBuffer {
    fn new() -> Self {
        TakeAllLinesBuffer {
            buffer: TakeAllBuffer::new(),
            lines: 0,
        }
    }

    fn fill_buffer(
        &mut self,
        reader: &mut impl Read,
        separator: u8,
    ) -> std::io::Result<BytesAndLines> {
        let bytes_read = self.buffer.fill_buffer(reader)?;
        // Count the number of lines...
        self.lines = memchr_iter(separator, self.buffer.remaining_buffer()).count();
        Ok(BytesAndLines {
            bytes: bytes_read,
            lines: self.lines,
        })
    }

    fn do_write_lines(
        &mut self,
        writer: &mut impl Write,
        max_lines: usize,
        separator: u8,
    ) -> std::io::Result<usize> {
        let index = memchr_iter(separator, self.buffer.remaining_buffer()).nth(max_lines - 1);
        assert!(
            index.is_some(),
            "Somehow we're being asked to write more lines than we have, that's a bug in the client."
        );
        let index = index.unwrap();
        // index is the offset of the separator character, zero indexed. Need to add 1 to get the number
        // of bytes to write.
        self.buffer.write_bytes_exact(writer, index + 1)?;
        Ok(index + 1)
    }

    fn write_lines(
        &mut self,
        writer: &mut impl Write,
        max_lines: usize,
        separator: u8,
    ) -> std::io::Result<BytesAndLines> {
        assert!(max_lines > 0, "Must request at least 1 line.");
        let ret;
        if max_lines > self.lines {
            ret = BytesAndLines {
                bytes: self.buffer.write_all(writer)?,
                lines: self.lines,
            };
            self.lines = 0;
        } else {
            ret = BytesAndLines {
                bytes: self.do_write_lines(writer, max_lines, separator)?,
                lines: max_lines,
            };
            self.lines -= max_lines;
        }
        Ok(ret)
    }

    fn remaining_bytes(&self) -> usize {
        self.buffer.remaining_bytes()
    }

    fn is_empty(&self) -> bool {
        self.remaining_bytes() == 0
    }

    fn lines(&self) -> usize {
        self.lines
    }
}

pub fn copy_all_but_lines<R: Read, W: Write>(
    mut reader: R,
    writer: &mut W,
    n: usize,
    separator: u8,
) -> std::io::Result<usize> {
    let mut buffers: VecDeque<TakeAllLinesBuffer> = VecDeque::new();
    let mut buffered_lines: usize = 0;
    let mut empty_buffers = vec![];
    let mut total_bytes_coppied = 0;
    loop {
        // Try to buffer enough such that we can write out the entire first buffer.
        loop {
            // First check if we have enough lines buffered that we can write out the entier
            // front buffer. If so, break.
            let front_buffer = buffers.front();
            if let Some(front_buffer) = front_buffer {
                if buffered_lines > n + front_buffer.lines() {
                    break;
                }
            }
            // We need to try to buffer more data...
            let mut new_buffer = empty_buffers.pop().unwrap_or_else(TakeAllLinesBuffer::new);
            let fill_result = new_buffer.fill_buffer(&mut reader, separator)?;
            if fill_result.bytes == 0 {
                // Must have hit EoF
                break;
            }
            buffered_lines += fill_result.lines;
            buffers.push_back(new_buffer);
        }

        // If we've not managed to buffer more lines than we need we must be done.
        if buffered_lines <= n {
            break;
        }

        // Since we have some data buffered, can assume we have at least 1 bufffer.
        let front_buffer = buffers.front_mut().unwrap();
        let excess_buffered_lines = buffered_lines - n;
        let write_result = front_buffer.write_lines(writer, excess_buffered_lines, separator)?;
        buffered_lines -= write_result.lines;
        total_bytes_coppied += write_result.bytes;
        // If the front buffer is empty (which it probably is), push it into the empty-buffer-pool.
        if front_buffer.is_empty() {
            empty_buffers.push(buffers.pop_front().unwrap());
        }
    }
    Ok(total_bytes_coppied)
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
