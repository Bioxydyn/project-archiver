from dataclasses import dataclass
from typing import List, Optional
import os
import time
import zipfile
import subprocess  # noqa
import copy


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
    def __init__(self):
        self._total_added_files = 0
        self._total_added_directories = 0
        self._total_added_size = 0

    def on_directory_tree_progress(
        self, added_files: List[FileMetadata], added_directories: List[DirectoryMetadata]
    ) -> None:
        self._total_added_files += len(added_files)
        self._total_added_directories += len(added_directories)
        self._total_added_size += sum(f.size for f in added_files)
        print(
            f"Added {self._total_added_files} files, {self._total_added_directories} directories,"
            f" {self._total_added_size} bytes"
        )


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


def create_full_listing(directory_tree: DirectoryTree) -> str:
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
            full_listing += f"{date_str} {bytes_to_human_padded(f.size)} {f.absolute_path}  {f.size}\n"
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

    return header + full_listing


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


def compress_chunk(chunk: DirectoryTree, chunk_number: int, output_directory: str) -> None:
    """
    Compress a chunk into a zip file.
    """
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    chunk_directory = output_directory + "/Chunks"

    if not os.path.exists(chunk_directory):
        os.makedirs(chunk_directory)

    listing = create_full_listing(chunk)

    zip_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}.zip")
    listing_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}Listing.txt")
    check_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}Check.txt")
    error_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}ERROR.txt")
    hash_file_name = os.path.join(chunk_directory, f"Chunk{chunk_number:07d}Hash.txt")

    # If any of the files already exist, then halt with an error
    if os.path.exists(zip_file_name) or os.path.exists(listing_file_name) or os.path.exists(check_file_name):
        raise RuntimeError("One or more output files already exist - resume is not supported. Aborting.")

    with open(listing_file_name, "w") as f:
        f.write(listing)

    input_files = get_all_files(chunk)

    with zipfile.ZipFile(zip_file_name, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for in_file in input_files:
            zip_file.write(in_file.absolute_path)

    with open(hash_file_name, "w") as f:
        f.write(get_sha_sum(zip_file_name))

    # Create the check file. Load the zip file from disk, and read all of the files. Check that every file in the
    # input is present, and the size of each file is correct.
    try:
        check_msg = check_chunk(chunk, zip_file_name)
        with open(check_file_name, "w") as f:
            f.write(check_msg)
    except Exception as e:
        with open(error_file_name, "w") as f:
            f.write(str(e))
        raise e


def check_chunk(chunk: DirectoryTree, zip_file_name: str) -> str:
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
            if file_in_input.absolute_path not in input_files_dict:
                raise RuntimeError(f"File {file_in_input.absolute_path} is not present in zip file {zip_file_name}.")

            if file_in_input.size != input_files_dict[file_in_input.absolute_path]:
                size_in_zip_str = bytes_to_human_padded(input_files_dict[file_in_input.absolute_path]).strip()
                size_in_input_str = bytes_to_human_padded(file_in_input.size).strip()
                raise RuntimeError(
                    f"File {file_in_input.absolute_path} has a different size in zip file ({size_in_zip_str}) than in"
                    f" the input chunk ({size_in_input_str})."
                )

        check_output += "All files in input chunk are present in zip file.\n\n"
        check_output += "All files in zip file have the correct size.\n\n"
        check_output += "Checks completed successfully.\n"

    return check_output
