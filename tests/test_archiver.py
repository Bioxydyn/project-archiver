import unittest
from unittest.mock import patch
import shutil
import os
import tempfile
from contextlib import contextmanager
import datetime
from typing import Optional, Generator
import random
import zipfile


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
    get_all_files,
    get_sha_sum,
    compress_chunk,
    check_chunk,
    create_chunk_dictionary,
    ArchiveRunner
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


def add_mock_files_many(
    n_folders: int = 3,
    folder_depth: int = 5,
    max_files_per_folder: int = 6,
    min_files_per_folder: int = 0,
    file_size_min: int = 5,
    file_size_max: int = 10
) -> None:

    os.mkdir("test")

    # Seed the random number generator so that we get the same results every time
    random.seed(0)

    def _recurse(base_folder: str, depth: int = 0) -> None:
        for i in range(n_folders):
            folder_name = f"{base_folder}/folder_{i}"
            os.mkdir(folder_name)
            if depth < folder_depth:
                _recurse(folder_name, depth + 1)

            files_per_folder = random.randint(min_files_per_folder, max_files_per_folder)  # noqa: S311
            for j in range(files_per_folder):
                with open(f"{folder_name}/file_{j}.txt", "w") as f:
                    this_file_size = random.randint(file_size_min, file_size_max)  # noqa: S311
                    f.write("-" * this_file_size)

    _recurse("test")


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

    def test_get_all_files_many(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            tree = build_directory_tree(".")
            files = get_all_files(tree)
            file_paths = [f.absolute_path for f in files]

            # Check there are no duplicates
            self.assertEqual(len(file_paths), len(set(file_paths)))

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
            header, listing = create_full_listing(tree, ".")
            todays_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            self.assertEqual(type(listing), str)
            expected_str = f"""
{todays_date_str} 1 Bytes    ./file_1.txt  1
{todays_date_str} 2 Bytes    ./file_2.txt  2
{todays_date_str} 40 Bytes   ./dir_1_lvl_1/file_3.txt  40
{todays_date_str} 80 Bytes   ./dir_1_lvl_1/dir_1_lvl_2/file_4.txt  80
{todays_date_str} 120 Bytes  ./dir_1_lvl_1/dir_1_lvl_2/dir_1_lvl_3/file_5.txt  120""".strip()

            self.assertIn(expected_str, listing)
            self.assertIn("Total Size: 243 Bytes", header)


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


class TestShaSum(unittest.TestCase):
    def test_sha_sum(self) -> None:
        with isolated_filesystem():
            add_mock_files()
            sha_sum = get_sha_sum("file_1.txt")
            self.assertIn("bb7208bc9b5d7c04f1236a82a0093a5e33f40423d5ba8d4266f7092c3ba43b62", sha_sum)
            self.assertIn("SHA256: ", sha_sum)


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

            chunked_files = {str([f.absolute_path for f in get_all_files(chunk)]) for chunk in chunks}
            expected_chunked_files = {
                "['./dir_1_lvl_1/dir_1_lvl_2/dir_1_lvl_3/file_5.txt']",
                "['./file_1.txt', './file_2.txt']",
                "['./dir_1_lvl_1/dir_1_lvl_2/file_4.txt']",
                "['./dir_1_lvl_1/file_3.txt']"
            }
            self.assertEqual(chunked_files, expected_chunked_files)

    def test_large_number_files_folders(self) -> None:
        with isolated_filesystem():
            add_mock_files_many(file_size_min=2, file_size_max=50)
            tree = build_directory_tree(".")
            settings = ChunkerSettings()
            settings.target_size_bytes = 15
            chunks = break_tree_into_chunks(tree, settings)
            total_chunked_size = sum([c.total_size_bytes for c in chunks])
            total_input_size = tree.total_size_bytes
            self.assertEqual(total_chunked_size, total_input_size)


class TestSaveChunks(unittest.TestCase):
    def test_save_chunks_smoke(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            tree = build_directory_tree("test")

            # Remove test_archive if it exists
            settings = ChunkerSettings()
            settings.target_size_bytes = 8000
            chunks = break_tree_into_chunks(tree, settings)
            for idx, chunk in enumerate(chunks):
                compress_chunk(chunk, idx, "test_archive", "test")
            header, complete_listing = create_full_listing(tree, "test")
            with open("test_archive/FullListing.txt", "w") as f:
                f.write(header)
                f.write(complete_listing)

            with self.assertRaisesRegex(RuntimeError, "One or more output files already exist"):
                compress_chunk(chunks[0], 0, "test_archive", "test")

            # Check that the files are there
            self.assertTrue(os.path.exists("test_archive/FullListing.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000001.zip"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000001Listing.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000001Check.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000001Hash.txt"))
            self.assertFalse(os.path.exists("test_archive/Chunks/Chunk0000001ERROR.txt"))

    def test_save_chunks_error(self) -> None:
        # Test that if there is an error, the error file is created and an exception is raised
        with isolated_filesystem():
            # Patch the check_chunk(chunk, filename) function to always raise an exception
            with patch("archiver.archiver.check_chunk", side_effect=RuntimeError("Test error")):
                add_mock_files_many()
                tree = build_directory_tree("test")
                settings = ChunkerSettings()
                settings.target_size_bytes = 8000
                chunks = break_tree_into_chunks(tree, settings)
                with self.assertRaisesRegex(RuntimeError, "Test error"):
                    compress_chunk(chunks[0], 0, "test_archive", "test")
                self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000ERROR.txt"))

    def test_verify_chunks(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            tree = build_directory_tree("test")
            settings = ChunkerSettings()
            settings.target_size_bytes = 8000
            chunks = break_tree_into_chunks(tree, settings)
            compress_chunk(chunks[0], 0, "test_archive", "test")
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000.zip"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Listing.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Check.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Hash.txt"))

            check_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

            chunks[0].directories[0].files[0].size += 1
            chunks[0].total_size_bytes += 1

            with self.assertRaisesRegex(RuntimeError, "Total size of files in zip file"):
                check_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

            chunks[0].directories[0].files[1].size -= 1
            chunks[0].total_size_bytes -= 1

            with self.assertRaisesRegex(RuntimeError, "File test/folder_1/file_0.txt has a different size in zip file"):
                check_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

            chunks[0].directories[0].files[1].size += 1
            chunks[0].directories[0].files[0].size -= 1
            check_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

            # Delete a file from the zip file
            file_0_contents: str = ""
            with zipfile.ZipFile("test_archive/Chunks/Chunk0000000.zip", "a") as zip_file:
                with zipfile.ZipFile("file_deleted.zip", "w") as zip_out:
                    for item in zip_file.infolist():
                        buffer = zip_file.read(item.filename)
                        if item.filename != "test/folder_1/file_0.txt":
                            zip_out.writestr(item, buffer)
                file_0_contents = zip_file.read("test/folder_1/file_0.txt").decode("utf-8")

            with self.assertRaisesRegex(RuntimeError, "Number of files"):
                check_chunk(chunks[0], "file_deleted.zip", "test")

            # Add a file to the zip file
            with zipfile.ZipFile("file_deleted.zip", "a") as zip_file:
                zip_file.writestr("test/folder_1/file_0_incorrect.txt", file_0_contents)

            with self.assertRaisesRegex(RuntimeError, "File test/folder_1/file_0.txt is not present"):
                check_chunk(chunks[0], "file_deleted.zip", "test")

    def test_chunk_dictionary(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            tree = build_directory_tree("test")
            settings = ChunkerSettings()
            settings.target_size_bytes = 8000
            chunks = break_tree_into_chunks(tree, settings)
            chunk_dict = create_chunk_dictionary(chunks, "test")
            self.assertEqual(type(chunk_dict), str)
            self.assertIn("Chunk 0000000: ", chunk_dict)
            self.assertIn("Chunk 0000001: ", chunk_dict)
            self.assertIn("Chunk 0000002: ", chunk_dict)
            self.assertTrue(len(chunk_dict) > 1000)


class TestArchiveRunner(unittest.TestCase):
    def test_archive_runner_good_input(self) -> None:
        runner = ArchiveRunner()
        with self.assertRaisesRegex(SystemExit, "0"):
            runner.parse_arguments(["--help"])

        with self.assertRaisesRegex(SystemExit, "0"):
            runner.parse_arguments(["--version"])

        with self.assertRaisesRegex(SystemExit, "2"):
            runner.parse_arguments(["--output-dir", "test_archive"])

        runner.parse_arguments(["--output-dir", "test_archive", "--input-dir", "test"])
        self.assertEqual(runner._input_directory, "test")
        self.assertEqual(runner._output_directory, "test_archive")
        self.assertEqual(runner._chunker_settings.target_size_bytes, ChunkerSettings().target_size_bytes)
        self.assertEqual(runner._verbose, False)
        runner.parse_arguments(["--output-dir", "test_archive", "--input-dir", "test", "--target-chunk-size-mb", "1"])
        self.assertEqual(runner._chunker_settings.target_size_bytes, 1 * 1024 * 1024)
        runner.parse_arguments(["--output-dir", "test_archive", "--input-dir", "test", "--verbose"])
        self.assertEqual(runner._verbose, True)
