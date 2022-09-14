import unittest
import shutil
import os
import tempfile
from contextlib import contextmanager
import datetime
from typing import Optional, Generator


from archiver.archiver import (
    bytes_to_human_padded,
    format_last_modified_time,
    list_all_files,
    FileMetadata,
    list_all_directories,
    DirectoryMetadata,
    build_directory_tree,
    DirectoryTree,
    create_full_listing,
    ChunkerSettings,
    ProgressPrinter,
    break_tree_into_chunks,
    get_all_directories,
    get_all_files
)


def add_mock_files() -> None:
    os.mkdir("dir_1_lvl_1")
    os.mkdir("dir_2_lvl_1")
    os.mkdir("dir_3_lvl_1")
    os.mkdir("dir_1_lvl_1/dir_1_lvl_2")
    os.mkdir("dir_1_lvl_1/dir_2_lvl_2")
    os.mkdir("dir_1_lvl_1/dir_1_lvl_2/dir_1_lvl_3")

    with open("file_1.txt", "w") as f:
        f.write("!" * 1)

    with open("file_2.txt", "w") as f:
        f.write("!" * 2)

    with open("dir_1_lvl_1/file_3.txt", "w") as f:
        f.write("test" * 10)

    with open("dir_1_lvl_1/dir_1_lvl_2/file_4.txt", "w") as f:
        f.write("test" * 20)

    with open("dir_1_lvl_1/dir_1_lvl_2/dir_1_lvl_3/file_5.txt", "w") as f:
        f.write("test" * 30)


@contextmanager
def isolated_filesystem(temp_path: Optional[str] = None) -> Generator:
    current_directory = os.getcwd()

    user_specified_path = temp_path is not None

    if not user_specified_path:
        temp_path = tempfile.mkdtemp()

    assert type(temp_path) is str

    try:
        os.chdir(temp_path)
        yield

    finally:
        os.chdir(current_directory)

        if not user_specified_path:
            shutil.rmtree(temp_path)


class TestFormatLastModified(unittest.TestCase):

    def test_format_last_modified(self) -> None:
        timestamp = 1663067974.3103588
        expected = "2022-09-13"
        actual = format_last_modified_time(timestamp)
        self.assertEqual(expected, actual)


class TestFormatBytes(unittest.TestCase):
    def test_zero(self) -> None:
        self.assertEqual(bytes_to_human_padded(0).strip(), "0 Bytes")

    def test_one(self) -> None:
        self.assertEqual(bytes_to_human_padded(1).strip(), "1 Bytes")

    def test_many(self) -> None:
        self.assertEqual(bytes_to_human_padded(1020).strip(), "1,020 Bytes")
        self.assertEqual(bytes_to_human_padded(1025).strip(), "1.00 KB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024).strip(), "1.00 MB")
        self.assertEqual(bytes_to_human_padded(int(1.4 * 1024 * 1024)).strip(), "1.40 MB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024 * 1024).strip(), "1.00 GB")
        self.assertEqual(bytes_to_human_padded(int(1.4 * 1024 * 1024 * 1024)).strip(), "1.40 GB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024 * 1024 * 1024).strip(), "1.00 TB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024 * 1024 * 1024 * 1024).strip(), "1,024.00 TB")

    def test_negative(self) -> None:
        with self.assertRaises(ValueError):
            bytes_to_human_padded(-1)


class TestListFiles(unittest.TestCase):
    def test_list_files(self) -> None:
        with isolated_filesystem():
            add_mock_files()

            files = list_all_files(".")
            self.assertEqual(len(files), 2)

            file_1 = [x for x in files if x.path == "file_1.txt"][0]
            file_2 = [x for x in files if x.path == "file_2.txt"][0]

            self.assertEqual(type(file_1), FileMetadata)
            self.assertEqual(file_1.size, 1)
            self.assertTrue(file_1.absolute_path.startswith("."))

            self.assertEqual(type(file_2), FileMetadata)
            self.assertEqual(file_2.size, 2)
            self.assertTrue(file_1.absolute_path.startswith("."))

            for file in files:
                self.assertTrue(os.path.isfile(file.path))
                self.assertTrue(os.path.isfile(file.absolute_path))

    def test_list_files_subdir(self) -> None:
        with isolated_filesystem():
            add_mock_files()

            files = list_all_files("dir_1_lvl_1")
            self.assertEqual(len(files), 1)

            file_1 = [x for x in files if x.path == "file_3.txt"][0]

            self.assertEqual(type(file_1), FileMetadata)
            self.assertEqual(file_1.size, 40)
            self.assertTrue(file_1.absolute_path.startswith("dir_1_lvl_1"))
            self.assertTrue(os.path.isfile(file_1.absolute_path))


class TestListDirectories(unittest.TestCase):
    def test_list_directories(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            dirs = list_all_directories(".")
            self.assertEqual(len(dirs), 3)
            dirs_1 = [x for x in dirs if x.path == "dir_1_lvl_1"][0]
            self.assertEqual(type(dirs_1), DirectoryMetadata)
            self.assertEqual(dirs_1.absolute_path, "./dir_1_lvl_1")


class TestBuildDirectoryTree(unittest.TestCase):
    def test_build_dir_tree_mock_data(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            self.assertEqual(type(tree), DirectoryTree)
            self.assertEqual(tree.path, ".")
            self.assertEqual(len(tree.files), 2)
            self.assertEqual(len(tree.directories), 3)
            self.assertEqual(
                {'./dir_2_lvl_1', './dir_1_lvl_1', './dir_3_lvl_1'},
                {d.path for d in tree.directories}
            )
            self.assertEqual(len(tree.directories[2].files), 1)
            self.assertEqual(len(tree.directories[0].files), 0)
            self.assertEqual(len(tree.directories[1].files), 0)
            self.assertEqual(len(tree.directories[2].directories), 2)

    def test_build_dir_tree_progress(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            progress_printer = ProgressPrinter()
            tree = build_directory_tree(".", progress_callback=progress_printer)
            self.assertEqual(type(tree), DirectoryTree)
            self.assertTrue(progress_printer._total_added_size > 0)

    def test_get_all_files(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            files = get_all_files(tree)
            self.assertEqual(len(files), 5)

    def test_get_all_dirs(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            dirs = get_all_directories(tree)
            self.assertEqual(len(dirs), 7)


class TestCreateListing(unittest.TestCase):
    def test_create_listing(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            print("\n\n")
            listing = create_full_listing(tree)
            todays_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            self.assertEqual(type(listing), str)
            expected_str = f"""
{todays_date_str} 1 Bytes    ./file_1.txt  1
{todays_date_str} 2 Bytes    ./file_2.txt  2
{todays_date_str} 40 Bytes   ./dir_1_lvl_1/file_3.txt  40
{todays_date_str} 80 Bytes   ./dir_1_lvl_1/dir_1_lvl_2/file_4.txt  80
{todays_date_str} 120 Bytes  ./dir_1_lvl_1/dir_1_lvl_2/dir_1_lvl_3/file_5.txt  120"""

            self.assertIn(expected_str, listing)


class TestChunkerSettings(unittest.TestCase):
    def test_chunker_settings(self) -> None:

        settings = ChunkerSettings()

        settings.target_size_bytes = 10
        settings.max_chunk_size_factor = 1.5
        settings.min_chunk_size_factor = 0.5

        self.assertEqual(settings.get_max_target_size_bytes(), 15)
        self.assertEqual(settings.get_min_target_size_bytes(), 5)

        str_settings = str(settings)

        expected_settings = """ChunkerSettings are:
        Target chunk size: 10 Bytes
        Target max chunk size: 15 Bytes
        Target min chunk size: 5 Bytes"""

        self.assertIn(expected_settings, str_settings)


class TestProgressPrinter(unittest.TestCase):
    def test_progress_printer(self) -> None:
        progress_printer = ProgressPrinter()

        progress_printer.on_directory_tree_progress(
            [
                FileMetadata(path="test", absolute_path="test", size=10, last_modified=0.0),
                FileMetadata(path="test2", absolute_path="test2", size=15, last_modified=0.0)
            ],
            [
                DirectoryMetadata(path="test", absolute_path="test"),
            ]
        )

        self.assertAlmostEqual(progress_printer._total_added_directories, 1)
        self.assertAlmostEqual(progress_printer._total_added_files, 2)
        self.assertAlmostEqual(progress_printer._total_added_size, 25)


class TestChunker(unittest.TestCase):
    def test_one_chunk(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            settings = ChunkerSettings()
            settings.target_size_bytes = 100000
            chunks = break_tree_into_chunks(tree, settings)
            self.assertEqual(len(chunks), 1)
            total_chunked_size = sum([c.total_size_bytes for c in chunks])
            total_input_size = tree.total_size_bytes
            self.assertEqual(total_chunked_size, total_input_size)

    def test_many_chunks(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            settings = ChunkerSettings()
            settings.target_size_bytes = 3
            chunks = break_tree_into_chunks(tree, settings)
            self.assertEqual(len(chunks), 4)
            total_chunked_size = sum([c.total_size_bytes for c in chunks])
            total_input_size = tree.total_size_bytes
            self.assertEqual(total_chunked_size, total_input_size)
