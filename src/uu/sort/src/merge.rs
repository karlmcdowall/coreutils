// This file is part of the uutils coreutils package.
//
// For the full copyright and license information, please view the LICENSE
// file that was distributed with this source code.
//! Merge already sorted files.
//!
//! We achieve performance by splitting the tasks of sorting and writing, and reading and parsing between two threads.
//! The threads communicate over channels. There's one channel per file in the direction reader -> sorter, but only
//! one channel from the sorter back to the reader. The channels to the sorter are used to send the read chunks.
//! The sorter reads the next chunk from the channel whenever it needs the next chunk after running out of lines
//! from the previous read of the file. The channel back from the sorter to the reader has two purposes: To allow the reader
//! to reuse memory allocations and to tell the reader which file to read from next.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    ffi::OsString,
    fs::{self, File},
    io::{BufWriter, Error, ErrorKind, Read, Write},
    iter,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    rc::Rc,
    sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender},
    thread::{self, JoinHandle},
};

use compare::Compare;
use uucore::error::{UIoError, UResult};

use crate::{
    chunks::{self, Chunk, RecycledChunk},
    compare_by, open,
    tmp_dir::TmpDirWrapper,
    GlobalSettings, Output, SortError,
};

/// If the output file occurs in the input files as well, copy the contents of the output file
/// and replace its occurrences in the inputs with that copy.
fn replace_output_file_in_input_files(
    files: &mut [OsString],
    output: Option<&str>,
    tmp_dir: &mut TmpDirWrapper,
) -> UResult<()> {
    let mut copy: Option<PathBuf> = None;
    if let Some(Ok(output_path)) = output.map(|path| Path::new(path).canonicalize()) {
        for file in files {
            if let Ok(file_path) = Path::new(file).canonicalize() {
                if file_path == output_path {
                    if let Some(copy) = &copy {
                        *file = copy.clone().into_os_string();
                    } else {
                        let (mut temp_file, copy_path) = tmp_dir.next_file()?;
                        let mut source_file = File::open(&file_path)?;
                        std::io::copy(&mut source_file, &mut temp_file)
                            .map_err(|error| SortError::OpenTmpFileFailed { error })?;
                        *file = copy_path.clone().into_os_string();
                        copy = Some(copy_path);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Merge pre-sorted `Box<dyn Read>`s.
///
/// If `settings.merge_batch_size` is greater than the length of `files`, intermediate files will be used.
/// If `settings.compress_prog` is `Some`, intermediate files will be compressed with it.
pub fn merge(
    files: &mut [OsString],
    settings: &GlobalSettings,
    output: Output,
    tmp_dir: &mut TmpDirWrapper,
) -> UResult<()> {
    replace_output_file_in_input_files(files, output.as_output_name(), tmp_dir)?;
    if settings.compress_prog.is_none() {
        merge_with_file_limit2::<WriteablePlainTmpFile>(files, settings, output, tmp_dir)
    } else {
        merge_with_file_limit2::<WriteableCompressedTmpFile>(files, settings, output, tmp_dir)
    }
}

pub fn merge_with_file_limit2<Tmp: WriteableTmpFile + 'static>(
    files: &mut [OsString],
    settings: &GlobalSettings,
    output: Output,
    tmp_dir: &mut TmpDirWrapper,
) -> UResult<()> {
    // First, merge down all the input files into temporary files.
    let mut input_file_names: VecDeque<&OsString> = files.iter().collect();
    let mut tmp_file = Tmp::create(tmp_dir.next_file()?, settings.compress_prog.as_deref())?;
    let mut temporary_files = VecDeque::new();

    // Code below assumes merge_batch_size is >=2. Assert it!
    assert!(settings.merge_batch_size >= 2);

    while !input_file_names.is_empty() {
        let mut opened_files = vec![];
        // First open some input files. Stop opening if we reach either...
        // 1 - the max merge batch size
        // 2 - the end of the input_file_names vector
        // 3 - the limit on the number of kernel file descriptors.
        loop {
            if opened_files.len() >= settings.merge_batch_size {
                // Check that we've not somehow accidentally violated our merge-size requirement.
                assert_eq!(opened_files.len(), settings.merge_batch_size);
                break;
            }
            let file_handle = match input_file_names.front() {
                Some(file_name) => open(file_name),
                None => break,
            };
            match file_handle {
                Ok(file) => {
                    opened_files.push(PlainMergeInput { inner: file });
                    input_file_names.pop_front();
                }
                //                Err(ref error) if error.error == std::io
                Err(err) => {
                    if opened_files.len() < 2 {
                        // We've hit any kind of error without opening at least 2 files. Just give up.
                        return Err(err);
                    }
                    //                    eprintln!("{}", err);
                    break;
                }
            }
        }

        // Now we should have a vector of input files, the size should be <= to our merge-size.
        assert!(opened_files.len() <= settings.merge_batch_size);

        // Above logic should ensure that we manage to open at least 2 files (assuming we have enough remaining input files. Assert it!
        assert!((opened_files.len() >= 2) || input_file_names.is_empty());

        // Todo - add optimization here to see if we can just bounce to the output without needing the temp file.
        let merger = merge_without_limit2(opened_files.into_iter(), settings)?;
        merger.write_all_to(settings, tmp_file.as_write())?;
        temporary_files.push_back(tmp_file.finished_writing()?);
        tmp_file = Tmp::create(tmp_dir.next_file()?, settings.compress_prog.as_deref())?;
    }
    merge_tmps_with_file_limit2::<Tmp>(temporary_files, settings, output, tmp_dir, tmp_file)
}

pub fn merge_tmps_with_file_limit2<Tmp: WriteableTmpFile + 'static>(
    mut input_temporary_files: VecDeque<Tmp::Closed>,
    settings: &GlobalSettings,
    output: Output,
    tmp_dir: &mut TmpDirWrapper,
    mut tmp_file: Tmp,
) -> UResult<()> {
    // Handle special case that we only have 1 temporary file.
    if input_temporary_files.len() == 1 {
        // Move it to the output... We can do this much more cleanly...
        // Reopen the temp file.
        let reopened_temp_file = iter::once(input_temporary_files.pop_front().unwrap().reopen()?);
        let merger = merge_without_limit2(reopened_temp_file, settings)?;
        return merger.write_all(settings, output);
    }
    // Bounce down all the temp files into as few temporary files as we can.
    let mut output_temporary_files = VecDeque::new();
    while !input_temporary_files.is_empty() {
        let mut opened_tmp_files = vec![];
        // First open some input files. Stop opening if we reach either...
        // 1 - the max merge batch size
        // 2 - the end of the input_file_names vector
        // 3 - the limit on the number of kernel fiole descriptors.
        while !input_temporary_files.is_empty() {
            if opened_tmp_files.len() >= settings.merge_batch_size {
                // Check that we've not somehow accidentally violated our merge-size requirement.
                assert_eq!(opened_tmp_files.len(), settings.merge_batch_size);
                // We have a full batch. Break out and merge them.
                break;
            }

            // Catch the case that we're on the last file and this is the first file in this batch.
            // Then just push it onto the back of the output files and be done.
            if input_temporary_files.len() ==1 && opened_tmp_files.is_empty() {
                output_temporary_files.push_back(input_temporary_files.pop_front().unwrap());
                break;
            }

            let copy_of_temp_file_to_reopen = match input_temporary_files.front() {
                Some(file) => file.clone(),
                None => break,
            };

            let temp_file_reopen_result = copy_of_temp_file_to_reopen.reopen();
            match temp_file_reopen_result {
                Ok(temp_file) => {
                    _ = input_temporary_files.pop_front();
                    opened_tmp_files.push(temp_file);
                }
                //                Err(ref error) if error.error == std::io
                Err(_err) => {
                    //                    eprintln!("{}", err);
                    // if opened_tmp_files.len() < 2 {
                    //     return Err(Box::new(UIoError::from(err)));
                    // }
                    break;
                }
            }
        }

        // Now we should have a vector of input files, the size should be <= to our merge-size.
        assert!(opened_tmp_files.len() <= settings.merge_batch_size);
        if opened_tmp_files.is_empty() {
            // Catch the case that we didn't open anything and just the file to the back of the output_temporary_files.
            assert!(input_temporary_files.is_empty());
            break;
        }

        //We should always have at least 2 input files here. Otherwise we should have bailed out of the loop earlier.
        assert!(opened_tmp_files.len() >= 2);
        let merger = merge_without_limit2(opened_tmp_files.into_iter(), settings)?;
        merger.write_all_to(settings, tmp_file.as_write())?;
        output_temporary_files.push_back(tmp_file.finished_writing()?);
        tmp_file = Tmp::create(tmp_dir.next_file()?, settings.compress_prog.as_deref())?;
    }
    // Now tail-call this function again. Eventually we should recurse until we have only
    // one file left which will finally be coppied to the output.
    merge_tmps_with_file_limit2::<Tmp>(output_temporary_files, settings, output, tmp_dir, tmp_file)
}

// Merge already sorted `MergeInput`s.
pub fn merge_with_file_limit<Tmp: WriteableTmpFile + 'static>(
    files: impl ExactSizeIterator<Item = UResult<impl MergeInput + 'static>>,
    settings: &GlobalSettings,
    output: Output,
    tmp_dir: &mut TmpDirWrapper,
) -> UResult<()> {
    if files.len() <= settings.merge_batch_size {
        let merger = merge_without_limit(files, settings)?;
        merger.write_all(settings, output)
    } else {
        let mut temporary_files = vec![];
        let mut batch = vec![];
        for file in files {
            batch.push(file);
            if batch.len() >= settings.merge_batch_size {
                assert_eq!(batch.len(), settings.merge_batch_size);
                let merger = merge_without_limit(batch.into_iter(), settings)?;
                batch = vec![];

                let mut tmp_file =
                    Tmp::create(tmp_dir.next_file()?, settings.compress_prog.as_deref())?;
                merger.write_all_to(settings, tmp_file.as_write())?;
                temporary_files.push(tmp_file.finished_writing()?);
            }
        }
        // Merge any remaining files that didn't get merged in a full batch above.
        if !batch.is_empty() {
            assert!(batch.len() < settings.merge_batch_size);
            let merger = merge_without_limit(batch.into_iter(), settings)?;

            let mut tmp_file =
                Tmp::create(tmp_dir.next_file()?, settings.compress_prog.as_deref())?;
            merger.write_all_to(settings, tmp_file.as_write())?;
            temporary_files.push(tmp_file.finished_writing()?);
        }
        merge_with_file_limit::<Tmp>(
            temporary_files
                .into_iter()
                .map(Box::new(|c: Tmp::Closed| c.reopen())
                    as Box<
                        dyn FnMut(Tmp::Closed) -> UResult<<Tmp::Closed as ClosedTmpFile>::Reopened>,
                    >),
            settings,
            output,
            tmp_dir,
        )
    }
}

/// Merge files without limiting how many files are concurrently open.
///
/// It is the responsibility of the caller to ensure that `files` yields only
/// as many files as we are allowed to open concurrently.
fn merge_without_limit<M: MergeInput + 'static, F: Iterator<Item = UResult<M>>>(
    files: F,
    settings: &GlobalSettings,
) -> UResult<FileMerger> {
    let (request_sender, request_receiver) = channel();
    let mut reader_files = Vec::with_capacity(files.size_hint().0);
    let mut loaded_receivers = Vec::with_capacity(files.size_hint().0);
    for (file_number, file) in files.enumerate() {
        let (sender, receiver) = sync_channel(2);
        loaded_receivers.push(receiver);
        reader_files.push(Some(ReaderFile {
            file: file?,
            sender,
            carry_over: vec![],
        }));
        // Send the initial chunk to trigger a read for each file
        request_sender
            .send((file_number, RecycledChunk::new(8 * 1024)))
            .unwrap();
    }

    // Send the second chunk for each file
    for file_number in 0..reader_files.len() {
        request_sender
            .send((file_number, RecycledChunk::new(8 * 1024)))
            .unwrap();
    }

    let reader_join_handle = thread::spawn({
        let settings = settings.clone();
        move || {
            reader(
                &request_receiver,
                &mut reader_files,
                &settings,
                settings.line_ending.into(),
            )
        }
    });

    let mut mergeable_files = vec![];

    for (file_number, receiver) in loaded_receivers.into_iter().enumerate() {
        if let Ok(chunk) = receiver.recv() {
            mergeable_files.push(MergeableFile {
                current_chunk: Rc::new(chunk),
                file_number,
                line_idx: 0,
                receiver,
            });
        }
    }

    Ok(FileMerger {
        heap: binary_heap_plus::BinaryHeap::from_vec_cmp(
            mergeable_files,
            FileComparator { settings },
        ),
        request_sender,
        prev: None,
        reader_join_handle,
    })
}

fn merge_without_limit2<F: Iterator<Item = impl MergeInput + 'static>>(
    files: F,
    settings: &GlobalSettings,
) -> UResult<FileMerger> {
    let (request_sender, request_receiver) = channel();
    let mut reader_files = Vec::with_capacity(files.size_hint().0);
    let mut loaded_receivers = Vec::with_capacity(files.size_hint().0);
    for (file_number, file) in files.enumerate() {
        let (sender, receiver) = sync_channel(2);
        loaded_receivers.push(receiver);
        reader_files.push(Some(ReaderFile {
            file,
            sender,
            carry_over: vec![],
        }));
        // Send the initial chunk to trigger a read for each file
        request_sender
            .send((file_number, RecycledChunk::new(8 * 1024)))
            .unwrap();
    }

    // Send the second chunk for each file
    for file_number in 0..reader_files.len() {
        request_sender
            .send((file_number, RecycledChunk::new(8 * 1024)))
            .unwrap();
    }

    let reader_join_handle = thread::spawn({
        let settings = settings.clone();
        move || {
            reader(
                &request_receiver,
                &mut reader_files,
                &settings,
                settings.line_ending.into(),
            )
        }
    });

    let mut mergeable_files = vec![];

    for (file_number, receiver) in loaded_receivers.into_iter().enumerate() {
        if let Ok(chunk) = receiver.recv() {
            mergeable_files.push(MergeableFile {
                current_chunk: Rc::new(chunk),
                file_number,
                line_idx: 0,
                receiver,
            });
        }
    }

    Ok(FileMerger {
        heap: binary_heap_plus::BinaryHeap::from_vec_cmp(
            mergeable_files,
            FileComparator { settings },
        ),
        request_sender,
        prev: None,
        reader_join_handle,
    })
}

/// The struct on the reader thread representing an input file
struct ReaderFile<M: MergeInput> {
    file: M,
    sender: SyncSender<Chunk>,
    carry_over: Vec<u8>,
}

/// The function running on the reader thread.
fn reader(
    recycled_receiver: &Receiver<(usize, RecycledChunk)>,
    files: &mut [Option<ReaderFile<impl MergeInput>>],
    settings: &GlobalSettings,
    separator: u8,
) -> UResult<()> {
    for (file_idx, recycled_chunk) in recycled_receiver {
        if let Some(ReaderFile {
            file,
            sender,
            carry_over,
        }) = &mut files[file_idx]
        {
            let should_continue = chunks::read(
                sender,
                recycled_chunk,
                None,
                carry_over,
                file.as_read(),
                &mut iter::empty(),
                separator,
                settings,
            )?;
            if !should_continue {
                // Remove the file from the list by replacing it with `None`.
                let ReaderFile { file, .. } = files[file_idx].take().unwrap();
                // Depending on the kind of the `MergeInput`, this may delete the file:
                file.finished_reading()?;
            }
        }
    }
    Ok(())
}
/// The struct on the main thread representing an input file
pub struct MergeableFile {
    current_chunk: Rc<Chunk>,
    line_idx: usize,
    receiver: Receiver<Chunk>,
    file_number: usize,
}

/// A struct to keep track of the previous line we encountered.
///
/// This is required for deduplication purposes.
struct PreviousLine {
    chunk: Rc<Chunk>,
    line_idx: usize,
    file_number: usize,
}

/// Merges files together. This is **not** an iterator because of lifetime problems.
struct FileMerger<'a> {
    heap: binary_heap_plus::BinaryHeap<MergeableFile, FileComparator<'a>>,
    request_sender: Sender<(usize, RecycledChunk)>,
    prev: Option<PreviousLine>,
    reader_join_handle: JoinHandle<UResult<()>>,
}

impl FileMerger<'_> {
    /// Write the merged contents to the output file.
    fn write_all(self, settings: &GlobalSettings, output: Output) -> UResult<()> {
        let mut out = output.into_write();
        self.write_all_to(settings, &mut out)
    }

    fn write_all_to(mut self, settings: &GlobalSettings, out: &mut impl Write) -> UResult<()> {
        while self.write_next(settings, out) {}
        drop(self.request_sender);
        self.reader_join_handle.join().unwrap()
    }

    fn write_next(&mut self, settings: &GlobalSettings, out: &mut impl Write) -> bool {
        if let Some(file) = self.heap.peek() {
            let prev = self.prev.replace(PreviousLine {
                chunk: file.current_chunk.clone(),
                line_idx: file.line_idx,
                file_number: file.file_number,
            });

            file.current_chunk.with_dependent(|_, contents| {
                let current_line = &contents.lines[file.line_idx];
                if settings.unique {
                    if let Some(prev) = &prev {
                        let cmp = compare_by(
                            &prev.chunk.lines()[prev.line_idx],
                            current_line,
                            settings,
                            prev.chunk.line_data(),
                            file.current_chunk.line_data(),
                        );
                        if cmp == Ordering::Equal {
                            return;
                        }
                    }
                }
                current_line.print(out, settings);
            });

            let was_last_line_for_file = file.current_chunk.lines().len() == file.line_idx + 1;

            if was_last_line_for_file {
                if let Ok(next_chunk) = file.receiver.recv() {
                    let mut file = self.heap.peek_mut().unwrap();
                    file.current_chunk = Rc::new(next_chunk);
                    file.line_idx = 0;
                } else {
                    self.heap.pop();
                }
            } else {
                // This will cause the comparison to use a different line and the heap to readjust.
                self.heap.peek_mut().unwrap().line_idx += 1;
            }

            if let Some(prev) = prev {
                if let Ok(prev_chunk) = Rc::try_unwrap(prev.chunk) {
                    // If nothing is referencing the previous chunk anymore, this means that the previous line
                    // was the last line of the chunk. We can recycle the chunk.
                    self.request_sender
                        .send((prev.file_number, prev_chunk.recycle()))
                        .ok();
                }
            }
        }
        !self.heap.is_empty()
    }
}

/// Compares files by their current line.
struct FileComparator<'a> {
    settings: &'a GlobalSettings,
}

impl Compare<MergeableFile> for FileComparator<'_> {
    fn compare(&self, a: &MergeableFile, b: &MergeableFile) -> Ordering {
        let mut cmp = compare_by(
            &a.current_chunk.lines()[a.line_idx],
            &b.current_chunk.lines()[b.line_idx],
            self.settings,
            a.current_chunk.line_data(),
            b.current_chunk.line_data(),
        );
        if cmp == Ordering::Equal {
            // To make sorting stable, we need to consider the file number as well,
            // as lines from a file with a lower number are to be considered "earlier".
            cmp = a.file_number.cmp(&b.file_number);
        }
        // BinaryHeap is a max heap. We use it as a min heap, so we need to reverse the ordering.
        cmp.reverse()
    }
}

// Wait for the child to exit and check its exit code.
fn check_child_success(mut child: Child, program: &str) -> UResult<()> {
    if matches!(
        child.wait().map(|e| e.code()),
        Ok(Some(0)) | Ok(None) | Err(_)
    ) {
        Ok(())
    } else {
        Err(SortError::CompressProgTerminatedAbnormally {
            prog: program.to_owned(),
        }
        .into())
    }
}

/// A temporary file that can be written to.
pub trait WriteableTmpFile: Sized {
    type Closed: ClosedTmpFile + Clone;
    type InnerWrite: Write;
    fn create(file: (File, PathBuf), compress_prog: Option<&str>) -> UResult<Self>;
    /// Closes the temporary file.
    fn finished_writing(self) -> UResult<Self::Closed>;
    fn as_write(&mut self) -> &mut Self::InnerWrite;
}

/// A temporary file that is (temporarily) closed, but can be reopened.
pub trait ClosedTmpFile {
    type Reopened: MergeInput;
    /// Reopens the temporary file.
    fn reopen(self) -> UResult<Self::Reopened>;
}

/// A pre-sorted input for merging.
pub trait MergeInput: Send {
    type InnerRead: Read;
    /// Cleans this `MergeInput` up.
    /// Implementations may delete the backing file.
    fn finished_reading(self) -> UResult<()>;
    fn as_read(&mut self) -> &mut Self::InnerRead;
}

pub struct WriteablePlainTmpFile {
    path: PathBuf,
    file: BufWriter<File>,
}
#[derive(Clone)]
pub struct ClosedPlainTmpFile {
    path: PathBuf,
}
pub struct PlainTmpMergeInput {
    path: PathBuf,
    file: File,
}
impl WriteableTmpFile for WriteablePlainTmpFile {
    type Closed = ClosedPlainTmpFile;
    type InnerWrite = BufWriter<File>;

    fn create((file, path): (File, PathBuf), _: Option<&str>) -> UResult<Self> {
        Ok(Self {
            file: BufWriter::new(file),
            path,
        })
    }

    fn finished_writing(self) -> UResult<Self::Closed> {
        Ok(ClosedPlainTmpFile { path: self.path })
    }

    fn as_write(&mut self) -> &mut Self::InnerWrite {
        &mut self.file
    }
}
impl ClosedTmpFile for ClosedPlainTmpFile {
    type Reopened = PlainTmpMergeInput;
    fn reopen(self) -> UResult<Self::Reopened> {
        Ok(PlainTmpMergeInput {
            file: File::open(&self.path).map_err(|error| SortError::OpenTmpFileFailed { error })?,
            path: self.path,
        })
    }
}
impl MergeInput for PlainTmpMergeInput {
    type InnerRead = File;

    fn finished_reading(self) -> UResult<()> {
        // we ignore failures to delete the temporary file,
        // because there is a race at the end of the execution and the whole
        // temporary directory might already be gone.
        let _ = fs::remove_file(self.path);
        Ok(())
    }

    fn as_read(&mut self) -> &mut Self::InnerRead {
        &mut self.file
    }
}

pub struct WriteableCompressedTmpFile {
    path: PathBuf,
    compress_prog: String,
    child: Child,
    child_stdin: BufWriter<ChildStdin>,
}
#[derive(Clone)]
pub struct ClosedCompressedTmpFile {
    path: PathBuf,
    compress_prog: String,
}
pub struct CompressedTmpMergeInput {
    path: PathBuf,
    compress_prog: String,
    child: Child,
    child_stdout: ChildStdout,
}
impl WriteableTmpFile for WriteableCompressedTmpFile {
    type Closed = ClosedCompressedTmpFile;
    type InnerWrite = BufWriter<ChildStdin>;

    fn create((file, path): (File, PathBuf), compress_prog: Option<&str>) -> UResult<Self> {
        let compress_prog = compress_prog.unwrap();
        let mut command = Command::new(compress_prog);
        command.stdin(Stdio::piped()).stdout(file);
        let mut child = command
            .spawn()
            .map_err(|err| SortError::CompressProgExecutionFailed {
                code: err.raw_os_error().unwrap(),
            })?;
        let child_stdin = child.stdin.take().unwrap();
        Ok(Self {
            path,
            compress_prog: compress_prog.to_owned(),
            child,
            child_stdin: BufWriter::new(child_stdin),
        })
    }

    fn finished_writing(self) -> UResult<Self::Closed> {
        drop(self.child_stdin);
        check_child_success(self.child, &self.compress_prog)?;
        Ok(ClosedCompressedTmpFile {
            path: self.path,
            compress_prog: self.compress_prog,
        })
    }

    fn as_write(&mut self) -> &mut Self::InnerWrite {
        &mut self.child_stdin
    }
}
impl ClosedTmpFile for ClosedCompressedTmpFile {
    type Reopened = CompressedTmpMergeInput;

    fn reopen(self) -> UResult<Self::Reopened> {
        let mut command = Command::new(&self.compress_prog);
        let file = File::open(&self.path).unwrap();
        command.stdin(file).stdout(Stdio::piped()).arg("-d");
        let mut child = command
            .spawn()
            .map_err(|err| SortError::CompressProgExecutionFailed {
                code: err.raw_os_error().unwrap(),
            })?;
        let child_stdout = child.stdout.take().unwrap();
        Ok(CompressedTmpMergeInput {
            path: self.path,
            compress_prog: self.compress_prog,
            child,
            child_stdout,
        })
    }
}
impl MergeInput for CompressedTmpMergeInput {
    type InnerRead = ChildStdout;

    fn finished_reading(self) -> UResult<()> {
        drop(self.child_stdout);
        check_child_success(self.child, &self.compress_prog)?;
        let _ = fs::remove_file(self.path);
        Ok(())
    }

    fn as_read(&mut self) -> &mut Self::InnerRead {
        &mut self.child_stdout
    }
}

pub struct PlainMergeInput<R: Read + Send> {
    inner: R,
}
impl<R: Read + Send> MergeInput for PlainMergeInput<R> {
    type InnerRead = R;
    fn finished_reading(self) -> UResult<()> {
        Ok(())
    }
    fn as_read(&mut self) -> &mut Self::InnerRead {
        &mut self.inner
    }
}
