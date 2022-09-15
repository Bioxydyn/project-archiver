from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple
import os
import time
import zipfile
import subprocess  # noqa
import copy
import argparse
from .version import __version__
from alive_progress import alive_bar  # type: ignore


def bytes_to_human_padded(n_bytes: int) -> str:
    if n_bytes < 0:
        raise ValueError("n_bytes must be >= 0")

    """
    Convert bytes to a human-readable string.
    """
    if n_bytes < 1024:
        return f"{n_bytes:,} Bytes".ljust(10)
    elif n_bytes < 1024 ** 2:
        return f"{n_bytes / 1024:,.2f} KB".ljust(10)
    elif n_bytes < 1024 ** 3:
        return f"{n_bytes / 1024 ** 2:,.2f} MB".ljust(10)
    elif n_bytes < 1024 ** 4:
        return f"{n_bytes / 1024 ** 3:,.2f} GB".ljust(10)
    else:
        return f"{n_bytes / 1024 ** 4:,.2f} TB".ljust(10)


def format_last_modified_time(last_modified: float) -> str:
    """
    Format a last modified time.
    """
    return time.strftime("%Y-%m-%d", time.localtime(last_modified))


@dataclass
class FileMetadata:
    path: str
    absolute_path: str
    size: int
    last_modified: float


@dataclass
class DirectoryMetadata:
    path: str
    absolute_path: str


@dataclass
class DirectoryTree:
    files: List[FileMetadata]
    total_size_bytes: int
    directories: List["DirectoryTree"]
    path: str
    absolute_path: str


@dataclass
class ChunkerSettings:
    target_size_bytes = 1024 * 1024 * 1024
    max_chunk_size_factor = 1.5
    min_chunk_size_factor = 0.5

    def __repr__(self) -> str:
        target_size_bytes_str = bytes_to_human_padded(self.target_size_bytes).strip()
        max_chunk_size_bytes_str = bytes_to_human_padded(self.get_max_target_size_bytes()).strip()
        min_chunk_size_bytes_str = bytes_to_human_padded(self.get_min_target_size_bytes()).strip()
        return f"""
        ChunkerSettings are:
        Target chunk size: {target_size_bytes_str}
        Target max chunk size: {max_chunk_size_bytes_str}
        Target min chunk size: {min_chunk_size_bytes_str}

        Note: Both min and max target sizes may be exceeded, the chunker will not split individual files into multiple
              chunks.
        """

    def get_max_target_size_bytes(self) -> int:
        return int(self.target_size_bytes * self.max_chunk_size_factor)

    def get_min_target_size_bytes(self) -> int:
        return int(self.target_size_bytes * self.min_chunk_size_factor)


class ProgressPrinter:
    def __init__(self, verbose: bool = False):
        self._total_added_files = 0
        self._total_added_directories = 0
        self._total_added_size = 0
        self._verbose = verbose
        self._alive_bar: Optional[Callable] = None
        self._last_update_time = 0

    def set_alive_bar(self, bar: Callable) -> None:  # noqa
        self._alive_bar = bar

    def on_directory_tree_progress(
        self, added_files: List[FileMetadata], added_directories: List[DirectoryMetadata]
    ) -> None:

        self._total_added_files += len(added_files)
        self._total_added_directories += len(added_directories)
        self._total_added_size += sum(f.size for f in added_files)
        if self._alive_bar is not None:
            self._alive_bar(len(added_files))
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        msg = (
            f"{time_str} Added {self._total_added_files} files, {self._total_added_directories} directories,"
            f" {bytes_to_human_padded(self._total_added_size).strip()}"
        )

        if self._verbose and int(time.time()) - self._last_update_time > 30:
            print(msg)
            self._last_update_time = int(time.time())


def list_all_files(path: str) -> List[FileMetadata]:
    """
    List all files in a directory (non-recursive).
    """
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    stats = [os.stat(os.path.join(path, f)) for f in files]
    files_metadata = [
        FileMetadata(
            path=f,
            absolute_path=os.path.join(path, f),
            size=stat.st_size,
            last_modified=stat.st_mtime
        )
        for f, stat in zip(files, stats)
    ]

    # Sort files_metadata by size, smallest to largest
    files_metadata.sort(key=lambda f: f.size)

    return files_metadata


def list_all_directories(path: str) -> List[DirectoryMetadata]:
    """
    List all directories in a directory (non-recursive).
    """
    return [
        DirectoryMetadata(path=d, absolute_path=os.path.join(path, d))
        for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))
    ]


def build_directory_tree(path: str, progress_callback: Optional[ProgressPrinter] = None) -> DirectoryTree:
    """
    Build a directory tree from a path.
    """
    files = list_all_files(path)
    directories = list_all_directories(path)

    if progress_callback:
        progress_callback.on_directory_tree_progress(files, directories)

    sub_trees = [build_directory_tree(d.absolute_path, progress_callback) for d in directories]

    # Sort sub_trees by total_size_bytes, smallest to largest
    sub_trees.sort(key=lambda t: t.total_size_bytes)

    total_size = sum(f.size for f in files) + sum(t.total_size_bytes for t in sub_trees)

    return DirectoryTree(
        files=files,
        total_size_bytes=total_size,
        directories=sub_trees,
        path=path,
        absolute_path=os.path.abspath(path)
    )


def get_all_files(directory_tree: DirectoryTree) -> List[FileMetadata]:
    """
    Get all files in a directory tree, recursively.
    """
    files = copy.deepcopy(directory_tree.files)
    for sub_tree in directory_tree.directories:
        files += get_all_files(sub_tree)
    return files


def get_all_directories(directory_tree: DirectoryTree) -> List[str]:
    """
    Get all directories in a directory tree, recursively.
    """
    directories = [directory_tree.absolute_path]
    for sub_tree in directory_tree.directories:
        directories += get_all_directories(sub_tree)
    return directories


def build_archive_path(input_directory: str, file: FileMetadata) -> str:
    """
    Build the archive path for a file. An input file /path/to/archive/file.txt will be archived as archive/file.txt.
    """
    last_folder_of_input = os.path.basename(input_directory)

    assert file.absolute_path.startswith(input_directory)
    return last_folder_of_input + file.absolute_path[len(input_directory):]


def create_full_listing(directory_tree: DirectoryTree, input_directory: str, line_prefix: str = "") -> Tuple[str, str]:
    """
    Create a full listing of a directory tree.
    """

    max_file_size_bytes: int = 0
    total_files: int = 0

    full_listing = ""

    # Recurve over the directory tree and print out the files and directories
    def _recurse(directory_tree: DirectoryTree) -> None:
        nonlocal total_files, max_file_size_bytes
        nonlocal full_listing
        total_files += len(directory_tree.files)
        for f in directory_tree.files:
            date_str = format_last_modified_time(f.last_modified)
            archive_path = build_archive_path(input_directory, f)
            full_listing += f"{line_prefix}{date_str} {bytes_to_human_padded(f.size)} {archive_path}  {f.size}\n"
            if f.size > max_file_size_bytes:
                max_file_size_bytes = f.size
        for d in directory_tree.directories:
            _recurse(d)

    _recurse(directory_tree)

    # Get the final directoy of the path
    directoy_name = directory_tree.absolute_path.split(os.sep)[-1]
    title = f"Directory Listing for: {directoy_name}"
    total_size_str = f"Total Size: { bytes_to_human_padded(directory_tree.total_size_bytes)}"
    total_files_str = f"Total Files: {total_files:,}"
    max_file_size_str = f"Max File Size: {bytes_to_human_padded(max_file_size_bytes)}"
    box_size = 100
    header = (
        "*" * box_size + "\n"
        + "*" + " " * (box_size - 2) + "*\n"
        + "*" + title.center(box_size - 2) + "*\n"
        + "*" + total_size_str.center(box_size - 2) + "*\n"
        + "*" + max_file_size_str.center(box_size - 2) + "*\n"
        + "*" + total_files_str.center(box_size - 2) + "*\n"
        + "*" + " " * (box_size - 2) + "*\n"
        + "*" * box_size + "\n\n"
        + "Printed on: " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "\n\n"
        + "Running with input directory: " + directory_tree.absolute_path + "\n\n"
    )

    return header, full_listing


def break_tree_into_chunks(directory_tree: DirectoryTree, chunker_settings: ChunkerSettings) -> List[DirectoryTree]:
    """
    Break a directory tree into chunks.
    """
    def _fresh_chunk(directory_tree: DirectoryTree) -> DirectoryTree:
        return DirectoryTree(
            files=[],
            total_size_bytes=0,
            directories=[],
            path=directory_tree.path,
            absolute_path=directory_tree.absolute_path
        )

    chunks: List[DirectoryTree] = []
    current_chunk = _fresh_chunk(directory_tree)
    chunks.append(current_chunk)

    def _recurse(directory_tree: DirectoryTree) -> None:
        nonlocal current_chunk
        nonlocal chunks

        for d in directory_tree.directories:
            if current_chunk.total_size_bytes + d.total_size_bytes > chunker_settings.get_max_target_size_bytes():
                if current_chunk.total_size_bytes > chunker_settings.get_min_target_size_bytes():
                    # Current chunk is finished, create a new chunk
                    current_chunk = _fresh_chunk(directory_tree)
                    chunks.append(current_chunk)

                    if d.total_size_bytes > chunker_settings.get_max_target_size_bytes():
                        # We need to split this directory up
                        _recurse(d)
                    else:
                        # We add this directory to the new chunk
                        current_chunk.directories.append(d)
                        current_chunk.total_size_bytes += d.total_size_bytes
                else:
                    # Current chunk is too small, but we can't add any more directories to it because
                    # each is too large to fit (we are iterating through directories ordered by size)
                    # We need to split this directory
                    _recurse(d)
            else:
                # When we add this directory, we don't exceed our max target size. So we can either add it to
                # the current chunk, or create a new chunk and add it to that.
                if current_chunk.total_size_bytes > chunker_settings.target_size_bytes:
                    current_chunk = _fresh_chunk(directory_tree)
                    chunks.append(current_chunk)

                current_chunk.directories.append(d)
                current_chunk.total_size_bytes += d.total_size_bytes

        for f in directory_tree.files:
            if current_chunk.total_size_bytes + f.size > chunker_settings.get_max_target_size_bytes():
                if current_chunk.total_size_bytes > chunker_settings.get_min_target_size_bytes():
                    # Current chunk is finished, create a new chunk
                    current_chunk = _fresh_chunk(directory_tree)
                    chunks.append(current_chunk)

                    current_chunk.files.append(f)
                    current_chunk.total_size_bytes += f.size
                else:
                    is_current_chunk_empty = current_chunk.total_size_bytes == 0
                    is_file_greater_than_target = f.size > chunker_settings.target_size_bytes

                    if is_file_greater_than_target and not is_current_chunk_empty:
                        current_chunk = _fresh_chunk(directory_tree)
                        chunks.append(current_chunk)

                    current_chunk.files.append(f)
                    current_chunk.total_size_bytes += f.size
            else:
                # When we add this file, we don't exceed our max target size. So we can either add it to
                # the current chunk, or create a new chunk and add it to that.
                if current_chunk.total_size_bytes > chunker_settings.target_size_bytes:
                    current_chunk = _fresh_chunk(directory_tree)
                    chunks.append(current_chunk)

                current_chunk.files.append(f)
                current_chunk.total_size_bytes += f.size

    _recurse(directory_tree)

    return chunks


def get_sha_sum(file_path: str) -> str:
    """
    Get the sha sum of a file
    """
    # Run shasum to get the checksum of the zip file
    return "SHA256: " + subprocess.run(  # noqa
        ["shasum", "-a", "256", file_path],
        stdout=subprocess.PIPE
    ).stdout.decode("utf-8")


def compress_chunk(chunk: DirectoryTree, chunk_number: int, output_directory: str, input_directory: str) -> None:
    """
    Compress a chunk into a zip file.
    """
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    chunk_directory = output_directory + "/Chunks"

    if not os.path.exists(chunk_directory):
        os.makedirs(chunk_directory)

    header, listing = create_full_listing(chunk, input_directory)

    zip_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}.zip")
    listing_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}Listing.txt")
    check_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}Check.txt")
    error_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}ERROR.txt")
    hash_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}Hash.txt")

    # If any of the files already exist, then halt with an error
    if os.path.exists(zip_file_name) or os.path.exists(listing_file_name) or os.path.exists(check_file_name):
        raise RuntimeError("One or more output files already exist - resume is not supported. Aborting.")

    with open(listing_file_name, "w") as f:
        f.write(header)
        f.write(listing)

    input_files = get_all_files(chunk)

    with zipfile.ZipFile(zip_file_name, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for in_file in input_files:
            # To form the arcname, remove the input directory from the start file path
            zip_file.write(in_file.absolute_path, arcname=build_archive_path(input_directory, in_file))

    with open(hash_file_name, "w") as f:
        f.write(get_sha_sum(zip_file_name))

    # Create the check file. Load the zip file from disk, and read all of the files. Check that every file in the
    # input is present, and the size of each file is correct.
    try:
        check_msg = check_chunk(chunk, zip_file_name, input_directory)
        with open(check_file_name, "w") as f:
            f.write(check_msg)
    except Exception as e:
        with open(error_file_name, "w") as f:
            f.write(str(e))
        raise e


def check_chunk(chunk: DirectoryTree, zip_file_name: str, input_directory: str) -> str:
    check_output: str = ""
    all_input_files: List[FileMetadata] = get_all_files(chunk)

    with zipfile.ZipFile(zip_file_name, "r") as zip_file:
        all_files: List[zipfile.ZipInfo] = zip_file.filelist

        # Check that the number of files in the zip file is the same as the number of files in the input
        if len(all_files) != len(all_input_files):
            raise RuntimeError(
                f"Number of files in zip file {zip_file_name} ({len(all_files)}) does not match number of"
                f" files in input ({len(all_input_files)})."
            )

        check_output += f"Found {len(all_files)} files in zip file.\n"
        check_output += f"Found {len(all_input_files)} files in input chunk.\n\n"

        # Check total size is the same
        total_size = 0
        for file_in_zip in all_files:
            total_size += file_in_zip.file_size

        if total_size != chunk.total_size_bytes:
            size_zip_str = bytes_to_human_padded(total_size).strip()
            size_input_str = bytes_to_human_padded(chunk.total_size_bytes).strip()
            raise RuntimeError(
                f"Total size of files in zip file {zip_file_name} ({size_zip_str}) does not match total size of files"
                f" in input chunk ({size_input_str})."
            )

        check_output += f"Total size of files in zip file: {bytes_to_human_padded(total_size).strip()} ({total_size:,}"
        check_output += " bytes).\n"
        check_output += f"Total size of files in input chunk:  {bytes_to_human_padded(chunk.total_size_bytes).strip()}"
        check_output += f" ({chunk.total_size_bytes:,} bytes).\n\n"

        # Check that each file in the input is present in the zip file
        input_files_dict = {f.filename: f.file_size for f in all_files}
        for file_in_input in all_input_files:
            arcname = build_archive_path(input_directory, file_in_input)
            if arcname not in input_files_dict:
                for file in input_files_dict.keys():
                    print(file)
                raise RuntimeError(f"File {arcname} is not present in zip file {zip_file_name}.")

            if file_in_input.size != input_files_dict[arcname]:
                size_in_zip_str = bytes_to_human_padded(input_files_dict[arcname]).strip()
                size_in_input_str = bytes_to_human_padded(file_in_input.size).strip()
                raise RuntimeError(
                    f"File {arcname} has a different size in zip file ({size_in_zip_str}) than in"
                    f" the input chunk ({size_in_input_str})."
                )

        check_output += "All files in input chunk are present in zip file.\n\n"
        check_output += "All files in zip file have the correct size.\n\n"
        check_output += "Checks completed successfully.\n"

    return check_output


def create_chunk_dictionary(chunks: List[DirectoryTree], input_directory: str) -> str:
    """
    Produce a dictionary of all files in the chunks.
    """
    chunk_dictionary: str = ""
    for idx, chunk in enumerate(chunks):
        line_prefix = f"Chunk {idx:07d}: "
        _, chunk_listing = create_full_listing(chunk, input_directory=input_directory, line_prefix=line_prefix)
        chunk_dictionary += chunk_listing
        chunk_dictionary += "\n\n\n"
    return chunk_dictionary


class ArchiveRunner:

    def __init__(self):
        self._chunker_settings = ChunkerSettings()
        self._input_directory: Optional[str] = None
        self._output_directory: Optional[str] = None

    def parse_arguments(self, args: List[str]) -> None:
        """
        Load arguments from argv, and parses them using argparse, supporting the following CLI:

        ./archiver --input-dir /path/to/input --output-dir /path/to/output --target-chunk-size-mb 1024

        input-dir and output-dir are mandatory. Target chunk size is optional
        """
        parser = argparse.ArgumentParser(description="Archive a directory into chunks of a given size.")
        parser.add_argument("--input-dir", type=str, required=True, help="Path to the input directory.")
        parser.add_argument("--output-dir", type=str, required=True, help="Path to the output directory.")
        parser.add_argument("--version", action="version", version=f"Project Archiver {__version__}")
        parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
        default_chunk_size_mb = int(self._chunker_settings.target_size_bytes / 1024 / 1024)
        parser.add_argument(
            "--target-chunk-size-mb",
            type=int,
            default=default_chunk_size_mb,
            help=f"Target size of each chunk in MB. Default is {default_chunk_size_mb} MB.",
        )

        parsed_args = parser.parse_args(args)

        self._input_directory = parsed_args.input_dir
        self._output_directory = parsed_args.output_dir
        self._chunker_settings.target_size_bytes = parsed_args.target_chunk_size_mb * 1024 * 1024
        self._verbose = parsed_args.verbose

    def run(self) -> None:
        progress_printer = ProgressPrinter(self._verbose)

        assert type(self._input_directory) is str
        assert type(self._output_directory) is str

        # Check that the input directory exists and is a directory
        if not os.path.exists(self._input_directory):
            raise RuntimeError(f"Input directory {self._input_directory} does not exist.")

        if not os.path.isdir(self._input_directory):
            raise RuntimeError(f"Input directory {self._input_directory} is not a directory.")

        # Check that the output directory exists and is an empty directory
        if not os.path.exists(self._output_directory):
            raise RuntimeError(f"Output directory {self._output_directory} does not exist.")

        if not os.path.isdir(self._output_directory):
            raise RuntimeError(f"Output directory {self._output_directory} is not a directory.")

        if len(os.listdir(self._output_directory)) > 0:
            raise RuntimeError(f"Output directory {self._output_directory} is not empty.")

        # Create initial directory tree
        with alive_bar(title_length=25, title="Scanning input dir", total=0) as bar:
            progress_printer.set_alive_bar(bar)
            input_tree = build_directory_tree(self._input_directory, progress_printer)

        # Create chunks
        with alive_bar(title_length=25, title="Deciding on chunks", total=0) as bar:
            progress_printer.set_alive_bar(bar)
            chunks = break_tree_into_chunks(input_tree, self._chunker_settings)

        # Save compressed chunks to disk
        with alive_bar(title_length=25, title="Saving chunks", total=len(chunks)) as bar:
            progress_printer.set_alive_bar(bar)
            for idx, chunk in enumerate(chunks):
                compress_chunk(chunk, idx, self._output_directory, self._input_directory)
                bar(idx / len(chunks))

        # Create dictionary of all files in chunks
        with alive_bar(title_length=25, title="Creating ChunkDictionary.txt", total=0) as bar:
            progress_printer.set_alive_bar(bar)
            chunk_dictionary = create_chunk_dictionary(chunks, self._input_directory)
            with open(os.path.join(self._output_directory, "ChunkDictionary.txt"), "w") as f:
                f.write(chunk_dictionary)

        # Create the full listing
        with alive_bar(title_length=25, title="Creating FullListing.txt", total=0) as bar:
            progress_printer.set_alive_bar(bar)
            header, content = create_full_listing(input_tree, self._input_directory)
            with open(os.path.join(self._output_directory, "FullListing.txt"), "w") as f:
                f.write(header)
                f.write(content)
