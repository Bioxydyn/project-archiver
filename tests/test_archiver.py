import unittest
import shutil
import os
import tempfile
from contextlib import contextmanager
import datetime

from archiver.archiver import (
    bytes_to_human_padded,
    format_last_modified_time,
    list_all_files,
    FileMetadata,
    list_all_directories,
    DirectoryMetadata,
    build_directory_tree,
    DirectoryTree,
    create_full_listing
)

def add_mock_files():
    os.mkdir("folder_1_lvl_1")
    os.mkdir("folder_2_lvl_1")
    os.mkdir("folder_3_lvl_1")
    os.mkdir("folder_1_lvl_1/folder_1_lvl_2")
    os.mkdir("folder_1_lvl_1/folder_2_lvl_2")
    os.mkdir("folder_1_lvl_1/folder_1_lvl_2/folder_1_lvl_3")

    with open("file_1.txt", "w") as f:
        f.write("!" * 1)
    
    with open("file_2.txt", "w") as f:
        f.write("!" * 2)

    with open("folder_1_lvl_1/file_3.txt", "w") as f:
        f.write("test" * 10)
    
    with open("folder_1_lvl_1/folder_2_lvl_2/file_4.txt", "w") as f:
        f.write("test" * 20)
    
    with open("folder_1_lvl_1/folder_1_lvl_2/folder_1_lvl_3/file_5.txt", "w") as f:
        f.write("test" * 30)

@contextmanager
def isolated_filesystem(temp_path: str = None):
    current_directory = os.getcwd()

    user_specified_path = False

    if temp_path is not None:
        user_specified_path = True

    if not user_specified_path:
        temp_path = tempfile.mkdtemp()

    try:
        os.chdir(temp_path)
        yield

    finally:
        os.chdir(current_directory)

        if not user_specified_path:
            shutil.rmtree(temp_path)


class TestFormatLastModified(unittest.TestCase):

    def test_format_last_modified(self):
        timestamp = 1663067974.3103588
        expected = "2022-09-13"
        actual = format_last_modified_time(timestamp)
        self.assertEqual(expected, actual)


class TestFormatBytes(unittest.TestCase):
    def test_zero(self):
        self.assertEqual(bytes_to_human_padded(0).strip(), "0 Bytes")
    
    def test_one(self):
        self.assertEqual(bytes_to_human_padded(1).strip(), "1 Bytes")
    
    def test_many(self):
        self.assertEqual(bytes_to_human_padded(1020).strip(), "1,020 Bytes")
        self.assertEqual(bytes_to_human_padded(1025).strip(), "1.00 KB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024).strip(), "1.00 MB")
        self.assertEqual(bytes_to_human_padded(1.4 * 1024 * 1024).strip(), "1.40 MB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024 * 1024).strip(), "1.00 GB")
        self.assertEqual(bytes_to_human_padded(1.4 * 1024 * 1024 * 1024).strip(), "1.40 GB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024 * 1024 * 1024).strip(), "1.00 TB")
        self.assertEqual(bytes_to_human_padded(1024 * 1024 * 1024 * 1024 * 1024).strip(), "1,024.00 TB")

    def test_negative(self):
        with self.assertRaises(ValueError):
            bytes_to_human_padded(-1)


class TestListFiles(unittest.TestCase):
    def test_list_files(self):
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


    def test_list_files_subdir(self):
        with isolated_filesystem():
            add_mock_files()

            files = list_all_files("folder_1_lvl_1")
            self.assertEqual(len(files), 1)

            file_1 = [x for x in files if x.path == "file_3.txt"][0]

            self.assertEqual(type(file_1), FileMetadata)
            self.assertEqual(file_1.size, 40)
            self.assertTrue(file_1.absolute_path.startswith("folder_1_lvl_1"))
            self.assertTrue(os.path.isfile(file_1.absolute_path))


class TestListDirectories(unittest.TestCase):
    def test_list_directories(self):
        with isolated_filesystem():
            add_mock_files()
            dirs = list_all_directories(".")
            self.assertEqual(len(dirs), 3)
            dirs_1 = [x for x in dirs if x.path == "folder_1_lvl_1"][0]
            self.assertEqual(type(dirs_1), DirectoryMetadata)
            self.assertEqual(dirs_1.absolute_path, "./folder_1_lvl_1")


class TestBuildDirectoryTree(unittest.TestCase):
    def test_build_dir_tree_mock_data(self):
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            self.assertEqual(type(tree), DirectoryTree)
            self.assertEqual(tree.path, ".")
            self.assertEqual(len(tree.files), 2)
            self.assertEqual(len(tree.directories), 3)
            self.assertEqual(set(['folder_2_lvl_1', 'folder_1_lvl_1', 'folder_3_lvl_1']), set(tree.directories.keys()))
            self.assertEqual(len(tree.directories['folder_1_lvl_1'].files), 1)
            self.assertEqual(len(tree.directories['folder_2_lvl_1'].files), 0)
            self.assertEqual(len(tree.directories['folder_3_lvl_1'].files), 0)
            self.assertEqual(len(tree.directories['folder_1_lvl_1'].directories.keys()), 2)


class TestCreateListing(unittest.TestCase):
    def test_create_listing(self):
        with isolated_filesystem():
            add_mock_files()
            tree = build_directory_tree(".")
            print("\n\n")
            listing = create_full_listing(tree)
            print("\n\n")
            print(listing)
            todays_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            self.assertEqual(type(listing), str)
            expected_str = f"""
{todays_date_str} - 1 Bytes    - file_1.txt
{todays_date_str} - 2 Bytes    - file_2.txt
    {todays_date_str} - 40 Bytes   - file_3.txt
        {todays_date_str} - 80 Bytes   - file_4.txt
            {todays_date_str} - 120 Bytes  - file_5.txt"""
            self.assertTrue(expected_str in listing)
