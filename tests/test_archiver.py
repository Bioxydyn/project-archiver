import unittest
from unittest.mock import patch
import shutil
import os
import tempfile
from contextlib import contextmanager
import datetime
from typing import Optional, Generator, Dict
import random
import zipfile
import json


from archiver.archiver import (
    format_bytes,
    format_last_modified_time,
    format_last_modified_time_as_iso,
    list_all_files,
    FileMetadata,
    list_all_directories,
    DirectoryMetadata,
    build_directory_tree,
    DirectoryTree,
    build_full_listing,
    ChunkerSettings,
    ProgressPrinter,
    divide_tree_into_chunks,
    get_all_directories,
    get_all_files,
    get_sha_sum,
    compress_chunk,
    verify_chunk,
    build_chunk_dictionary,
    ArchiveRunner,
    build_react_chonky_json_listing
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

    with open("dir_1_lvl_1/.file_3.txt", "w") as f:
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


global_objects_added = {}


class MockBoto3Bucket:
    """
    Mocks a boto3 bucket for testing.
    """
    def __init__(self, name: str):
        self.name = name
        self.objects: Dict[str, str] = {}

    def upload_file(self, filename: str, key: str) -> None:
        global global_objects_added
        self.objects[key] = filename
        global_objects_added[key] = filename


class MockBoto3Resource:
    """
    A mock boto3 resource for testing.
    """
    def __init__(self, service_name: str, endpoint_url: str):
        self.service_name = service_name
        self.endpoint_url = endpoint_url
        self.buckets: Dict[str, MockBoto3Bucket] = {}

    def Bucket(self, name: str) -> MockBoto3Bucket:
        self.name = name
        self.buckets[name] = MockBoto3Bucket(name)
        return self.buckets[name]


class MockBoto3Session:
    """
    A mock boto3 session for testing.
    """
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.resources: Dict[str, MockBoto3Resource] = {}

    def resource(self, service_name: str, endpoint_url: str) -> MockBoto3Resource:
        self.service_name = service_name
        self.endpoint_url = endpoint_url
        self.resources[self.service_name] = MockBoto3Resource(service_name, endpoint_url)
        return self.resources[self.service_name]


class TestFormatLastModified(unittest.TestCase):

    def test_format_last_modified(self) -> None:
        timestamp = 1663067974.3103588
        expected = "2022-09-13"
        actual = format_last_modified_time(timestamp)
        self.assertEqual(expected, actual)

    def test_format_last_modified_time_as_iso(self) -> None:
        timestamp = 1663067974.3103588
        expected = "2022-09-13T"
        actual = format_last_modified_time_as_iso(timestamp)
        self.assertIn(expected, actual)


class TestFormatBytes(unittest.TestCase):
    def test_zero(self) -> None:
        self.assertEqual(format_bytes(0).strip(), "0 Bytes")

    def test_one(self) -> None:
        self.assertEqual(format_bytes(1).strip(), "1 Bytes")

    def test_many(self) -> None:
        self.assertEqual(format_bytes(1020).strip(), "1,020 Bytes")
        self.assertEqual(format_bytes(1025).strip(), "1.00 KB")
        self.assertEqual(format_bytes(1024 * 1024).strip(), "1.00 MB")
        self.assertEqual(format_bytes(int(1.4 * 1024 * 1024)).strip(), "1.40 MB")
        self.assertEqual(format_bytes(1024 * 1024 * 1024).strip(), "1.00 GB")
        self.assertEqual(format_bytes(int(1.4 * 1024 * 1024 * 1024)).strip(), "1.40 GB")
        self.assertEqual(format_bytes(1024 * 1024 * 1024 * 1024).strip(), "1.00 TB")
        self.assertEqual(format_bytes(1024 * 1024 * 1024 * 1024 * 1024).strip(), "1,024.00 TB")

    def test_negative(self) -> None:
        with self.assertRaises(ValueError):
            format_bytes(-1)


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

            file_1 = [x for x in files if x.path == ".file_3.txt"][0]

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
            header, listing = build_full_listing(tree, ".")
            todays_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            self.assertEqual(type(listing), str)
            expected_str = f"""
{todays_date_str} 1 Bytes    ./file_1.txt  1
{todays_date_str} 2 Bytes    ./file_2.txt  2
{todays_date_str} 40 Bytes   ./dir_1_lvl_1/.file_3.txt  40
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
                DirectoryMetadata(path="test", absolute_path="test", last_modified=0.0),
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
            chunks = divide_tree_into_chunks(tree, settings)
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
            chunks = divide_tree_into_chunks(tree, settings)
            self.assertEqual(len(chunks), 4)
            total_chunked_size = sum([c.total_size_bytes for c in chunks])
            total_input_size = tree.total_size_bytes
            self.assertEqual(total_chunked_size, total_input_size)

            chunked_files = {str([f.absolute_path for f in get_all_files(chunk)]) for chunk in chunks}
            expected_chunked_files = {
                "['./dir_1_lvl_1/dir_1_lvl_2/dir_1_lvl_3/file_5.txt']",
                "['./file_1.txt', './file_2.txt']",
                "['./dir_1_lvl_1/dir_1_lvl_2/file_4.txt']",
                "['./dir_1_lvl_1/.file_3.txt']"
            }
            self.assertEqual(chunked_files, expected_chunked_files)

    def test_large_number_files_folders(self) -> None:
        with isolated_filesystem():
            add_mock_files_many(file_size_min=2, file_size_max=50)
            tree = build_directory_tree(".")
            settings = ChunkerSettings()
            settings.target_size_bytes = 15
            chunks = divide_tree_into_chunks(tree, settings)
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
            chunks = divide_tree_into_chunks(tree, settings)
            for idx, chunk in enumerate(chunks):
                compress_chunk(chunk, idx, "test_archive", "test")
            header, complete_listing = build_full_listing(tree, "test")
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
            # Patch the verify_chunk(chunk, filename) function to always raise an exception
            with patch("archiver.archiver.verify_chunk", side_effect=RuntimeError("Test error")):
                add_mock_files_many()
                tree = build_directory_tree("test")
                settings = ChunkerSettings()
                settings.target_size_bytes = 8000
                chunks = divide_tree_into_chunks(tree, settings)
                with self.assertRaisesRegex(RuntimeError, "Test error"):
                    compress_chunk(chunks[0], 0, "test_archive", "test")
                self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000ERROR.txt"))

    def test_verify_chunks(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            tree = build_directory_tree("test")
            settings = ChunkerSettings()
            settings.target_size_bytes = 8000
            chunks = divide_tree_into_chunks(tree, settings)
            compress_chunk(chunks[0], 0, "test_archive", "test")
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000.zip"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Listing.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Check.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Hash.txt"))

            verify_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

            chunks[0].directories[0].files[0].size += 1
            chunks[0].total_size_bytes += 1

            with self.assertRaisesRegex(RuntimeError, "Total size of files in zip file"):
                verify_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

            chunks[0].directories[0].files[1].size -= 1
            chunks[0].total_size_bytes -= 1

            with self.assertRaisesRegex(RuntimeError, "File test/folder_1/file_0.txt has a different size in zip file"):
                verify_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

            chunks[0].directories[0].files[1].size += 1
            chunks[0].directories[0].files[0].size -= 1
            verify_chunk(chunks[0], "test_archive/Chunks/Chunk0000000.zip", "test")

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
                verify_chunk(chunks[0], "file_deleted.zip", "test")

            # Add a file to the zip file
            with zipfile.ZipFile("file_deleted.zip", "a") as zip_file:
                zip_file.writestr("test/folder_1/file_0_incorrect.txt", file_0_contents)

            with self.assertRaisesRegex(RuntimeError, "File test/folder_1/file_0.txt is not present"):
                verify_chunk(chunks[0], "file_deleted.zip", "test")

    def test_chunk_dictionary(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            tree = build_directory_tree("test")
            settings = ChunkerSettings()
            settings.target_size_bytes = 8000
            chunks = divide_tree_into_chunks(tree, settings)
            chunk_dict = build_chunk_dictionary(chunks, "test")
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

        with self.assertRaisesRegex(ValueError, "Target chunk size must be at least 1"):
            runner.parse_arguments(
                ["--output-dir", "test_archive", "--input-dir", "test", "--target-chunk-size-mb", "0"]
            )

    def test_archive_runner_run(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            runner = ArchiveRunner()
            runner.parse_arguments(["--output-dir", "test_archive", "--input-dir", "test"])

            with self.assertRaisesRegex(RuntimeError, "does not exist"):
                # Output folder doesn't exist
                runner.run()
            os.mkdir("test_archive")
            os.mkdir("test_archive/Chunks")
            with self.assertRaisesRegex(RuntimeError, "is not empty"):
                # Output folder not empty
                runner.run()
            os.rmdir("test_archive/Chunks")

            runner.parse_arguments(["--output-dir", "test_archive", "--input-dir", "test_not_exist"])
            with self.assertRaisesRegex(RuntimeError, "does not exist"):
                # Input folder doesn't exist
                runner.run()

            # Create a file called "test" in the current directory
            with open("test_not_exist", "w") as file:
                file.write("test")
            with self.assertRaisesRegex(RuntimeError, "is not a directory"):
                # Input folder isn't a folder
                runner.run()

            runner.parse_arguments(["--output-dir", "test_in", "--input-dir", "test"])
            # Create a file called "test" in the current directory
            with open("test_in", "w") as file:
                file.write("test")

            with self.assertRaisesRegex(RuntimeError, "is not a directory"):
                # Output folder isn't a folder
                runner.run()

            runner.parse_arguments(
                [
                    "--output-dir", "test_archive", "--input-dir", "test", "--verbose",
                    "--target-chunk-size-mb", "0.1",
                ]
            )

            runner.run()

            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000.zip"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Listing.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Check.txt"))
            self.assertTrue(os.path.exists("test_archive/Chunks/Chunk0000000Hash.txt"))
            self.assertTrue(os.path.exists("test_archive/ChunkDictionary.txt"))
            self.assertTrue(os.path.exists("test_archive/FullListing.txt"))

    def test_archive_runner_upload(self) -> None:
        # patch boto3.Session to return a mock session
        with isolated_filesystem():
            add_mock_files_many()
            runner = ArchiveRunner()
            runner.parse_arguments(
                [
                    "--output-dir", "test_archive", "--input-dir", "test", "--verbose",
                    "--target-chunk-size-mb", "0.01", "--upload"
                ]
            )
            self.assertTrue(runner._upload)
            os.mkdir("test_archive")

            os.environ.pop("ARCHIVER_S3_ACCESS_KEY", None)
            os.environ.pop("ARCHIVER_S3_SECRET_KEY", None)
            os.environ.pop("ARCHIVER_S3_BUCKET_NAME", None)
            os.environ.pop("ARCHIVER_S3_ENDPOINT_URL", None)

            with self.assertRaisesRegex(RuntimeError, "ARCHIVER_S3_ACCESS_KEY"):
                runner.run(boto_session_cls=MockBoto3Session)

            os.environ["ARCHIVER_S3_ACCESS_KEY"] = "test_access_key"

            with self.assertRaisesRegex(RuntimeError, "ARCHIVER_S3_SECRET_KEY"):
                runner.run(boto_session_cls=MockBoto3Session)

            os.environ["ARCHIVER_S3_SECRET_KEY"] = "test_secret_key"  # noqa

            with self.assertRaisesRegex(RuntimeError, "ARCHIVER_S3_BUCKET_NAME"):
                runner.run(boto_session_cls=MockBoto3Session)

            os.environ["ARCHIVER_S3_BUCKET_NAME"] = "test_bucket"

            with self.assertRaisesRegex(RuntimeError, "ARCHIVER_S3_ENDPOINT_URL"):
                runner.run(boto_session_cls=MockBoto3Session)

            os.environ["ARCHIVER_S3_ENDPOINT_URL"] = "test_endpoint_url"

            # Now all the environment variables are set, so it should run
            runner.run(boto_session_cls=MockBoto3Session)

            global global_objects_added

            for blob_name, file_name in global_objects_added.items():
                self.assertTrue(blob_name.startswith("test/"))
                self.assertTrue(os.path.exists(file_name))
                # Get the filename of the blob_name
                blob_filename = blob_name.split("/")[-1]
                self.assertIn(blob_filename, file_name)

            self.assertIn("test/ChunkDictionary.txt", global_objects_added)
            self.assertIn("test/FullListing.txt", global_objects_added)

            self.assertIn("test/Chunks/Chunk0000000.zip", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000000Listing.txt", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000000Check.txt", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000000Hash.txt", global_objects_added)

            self.assertIn("test/Chunks/Chunk0000001.zip", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000001Listing.txt", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000001Check.txt", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000001Hash.txt", global_objects_added)

            self.assertIn("test/Chunks/Chunk0000002.zip", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000002Listing.txt", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000002Check.txt", global_objects_added)
            self.assertIn("test/Chunks/Chunk0000002Hash.txt", global_objects_added)

            # With trailing / on output-dir and input-dir
            runner.parse_arguments(
                [
                    "--output-dir", "test_archive/", "--input-dir", "test/", "--verbose",
                    "--target-chunk-size-mb", "0.01", "--upload"
                ]
            )
            global_objects_added = {}
            shutil.rmtree("test_archive")
            os.mkdir("test_archive")
            runner.run(boto_session_cls=MockBoto3Session)

            for blob_name, file_name in global_objects_added.items():
                self.assertTrue(blob_name.startswith("test/"))
                self.assertTrue(os.path.exists(file_name))
                # Get the filename of the blob_name
                blob_filename = blob_name.split("/")[-1]
                self.assertIn(blob_filename, file_name)

            # With absolute paths
            current_dir = os.path.abspath(os.path.curdir)
            print(current_dir)
            runner.parse_arguments(
                [
                    "--output-dir", os.path.join(current_dir, "test_archive/"),
                    "--input-dir", os.path.join(current_dir, "test/"), "--verbose",
                    "--target-chunk-size-mb", "0.01", "--upload"
                ]
            )
            global_objects_added = {}
            shutil.rmtree("test_archive")
            os.mkdir("test_archive")
            runner.run(boto_session_cls=MockBoto3Session)

            for blob_name, file_name in global_objects_added.items():
                self.assertTrue(blob_name.startswith("test/"))
                self.assertTrue(os.path.exists(file_name))
                # Get the filename of the blob_name
                blob_filename = blob_name.split("/")[-1]
                self.assertIn(blob_filename, file_name)

    def test_archive_runner_run_html_only(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            runner = ArchiveRunner()
            runner.parse_arguments(["--output-dir", "test_archive", "--input-dir", "test", "--html-only"])
            os.mkdir("test_archive")
            runner.run()
            self.assertTrue(os.path.exists("test_archive/WebInterface.html"))
            self.assertFalse(os.path.exists("test_archive/FullListing.txt"))

            with self.assertRaisesRegex(RuntimeError, "Cannot enable --upload and output HTML only."):
                runner.parse_arguments(
                    ["--output-dir", "test_archive", "--input-dir", "test", "--html-only", "--upload"]
                )


class TestChonkyJSONExport(unittest.TestCase):
    def test_chonky_json_export(self) -> None:
        with isolated_filesystem():
            add_mock_files_many()
            tree = build_directory_tree("test")
            settings = ChunkerSettings()
            settings.target_size_bytes = 80
            _ = divide_tree_into_chunks(tree, settings)
            export = build_react_chonky_json_listing(tree, "test")
            stringified = json.dumps(export, indent=4)
            self.assertTrue(len(stringified) > 0)
