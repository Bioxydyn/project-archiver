from dataclasses import dataclass
from typing import List, Dict, Optional
import os
import time


class ProgressPrinter:
    def __init__(self):
        self._total_added_files = 0
        self._total_added_folders = 0
        self._total_added_size = 0

    def on_build_directory_tree_progress(self, n_added_files: int, n_added_folders: int, n_added_size: int):
        self._total_added_files += n_added_files
        self._total_added_folders += n_added_folders
        self._total_added_size += n_added_size
        print(f"Added {self._total_added_files} files, {self._total_added_folders} folders, {self._total_added_size} bytes")


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
    total_file_size: int
    directories: Dict[str, "DirectoryTree"]
    path: str
    absolute_path: str


def list_all_files(path: str) -> List[FileMetadata]:
    """
    List all files in a directory (non-recursive).
    """
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    stats = [os.stat(os.path.join(path, f)) for f in files]
    return [
        FileMetadata(
            path=f,
            absolute_path=os.path.join(path, f),
            size=stat.st_size,
            last_modified=stat.st_mtime
        )
        for f, stat in zip(files, stats)
    ]


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
    total_file_size = sum(f.size for f in files)
    
    if progress_callback:
        progress_callback.on_build_directory_tree_progress(len(files), len(directories), total_file_size)

    return DirectoryTree(
        files=files,
        total_file_size=total_file_size,
        directories={
            d.path: build_directory_tree(d.absolute_path)
            for d in directories
        },
        path=path,
        absolute_path=os.path.abspath(path)
    )


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


def create_full_listing(directory_tree: DirectoryTree) -> str:
    """
    Create a full listing of a directory tree.
    """

    total_size_bytes: int = 0
    total_files: int = 0

    full_listing = ""

    # Recurve over the directory tree and print out the files and directories
    def _recurse(directory_tree: DirectoryTree, indent: int = 0):
        nonlocal total_size_bytes, total_files
        nonlocal full_listing
        total_files += len(directory_tree.files)
        total_size_bytes += directory_tree.total_file_size
        for f in directory_tree.files:
            date_str = format_last_modified_time(f.last_modified)
            full_listing += f"{indent * ' '}{date_str} - {bytes_to_human_padded(f.size)} - {f.path}\n"
        for d in directory_tree.directories.values():
            _recurse(d, indent + 4)

    _recurse(directory_tree)
    
    # Get the final folder of the path
    folder_name = directory_tree.absolute_path.split(os.sep)[-1]

    title = f"Directory Listing for: {folder_name}"
    total_size_str = f"Total Size: { bytes_to_human_padded(total_size_bytes)}"
    total_files_str = f"Total Files: {total_files:,}"
    box_size = 100
    header = (
        "*" * box_size + "\n" +
        "*" + " " * (box_size - 2) + "*\n" + 
        "*" + title.center(box_size - 2) + "*\n" + 
        "*" + total_size_str.center(box_size - 2) + "*\n" + 
        "*" + total_files_str.center(box_size - 2) + "*\n" + 
        "*" + " " * (box_size - 2) + "*\n" +
        "*" * box_size + "\n\n"
        "Printed on: " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "\n\n"
        "Running with input folder: " + directory_tree.absolute_path + "\n\n"
    )

    print(header)

    return header + full_listing
