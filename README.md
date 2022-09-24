# Project Archiver

[![Coverage Status](https://coveralls.io/repos/github/Bioxydyn/project-archiver/badge.svg?branch=main)](https://coveralls.io/github/Bioxydyn/project-archiver?branch=main)

What does it do?
----------------

- Compress a folder containing many files & sub-folders into a set of chunks (zip files)
- Upload chunks to S3-compatible cloud storage
- Create a static, single-page web interface that allows "browsing" the archived folder & viewing which files are stored in which chunks.

Why would you want to use it?
-----------------------------

- You want to archive a folder to a cloud storage platform
- The folder contains millions of small files
- You want to:
    - Compress the files before uploading
    - Avoid storing the files individually, for both performance & cost see e.g., [here](https://aws.amazon.com/s3/pricing/#:~:text=S3%20Glacier%20Instant%20Retrieval%20has,object%20size%20of%20128%20KB.)
    - Easily browse the files at any point after archiving them
    - Easily access individual files without having to download everything

Installation instructions
-------------------------

    pip3 install git+https://github.com/Bioxydyn/project-archiver.git@main

Example
-------

    archiver --help
    archiver --input-dir /path/to/myProject -output-dir /path/to/archive --verbose --upload

Will perform the following:

1. Scan the input folder and all files in it. For each file will save the relative path, size and last modified date.
2. Save out this initial listing to `/path/to/archive/FullListing.txt`.
3. Make a decision to split which files into which chunks. This is based on the size of the files and the size of the chunks. The default chunk size is 1GB, but this can be changed with the `--target-chunk-size-mb` option.
4. Go through each of the chunks and make a zip file, listing and SHA256 hash for each, producing the following files:
    - `/path/to/archive/Chunks/Chunk00001Listing.txt`
    - `/path/to/archive/Chunks/Chunk00001.zip`
    - `/path/to/archive/Chunks/Chunk00001Hash.txt`
5. Read back the zip file and check it contains all expected files and their size is as expected from the full listing, producing the following file:
    - `/path/to/archive/Chunks/Chunk00001Check.txt`
6. Save a list of all files and to which chunk they belong to `/path/to/archive/ChunkDictionary.txt`. This file will allow a future reader to identify which of the files in the input are stored in which chunks, thus allowing them to access them without downloading the full archive.
7. Save a self-contained web interface to the archive in `/path/to/archive/WebInterface.html`. This can be used to browse the archive and see which files are in which chunks. Alternatively it can easily be shared with a collaborator without providing them access to the full archive.
8.  If `--upload` is set, upload the chunks to the configured S3-compatible storage bucket. For further details see `archiver --help`.

Notes: Project Archiver will attempt to split the files into sensible chunks based on the size of the files and the size of the chunks. It will prefer to split folders into standalone chunks, and prefer to split at a higher level in the directory tree if possible. It will try to avoid splitting files that existing within the same directory into different chunks.

It will try to split the files into chunks of the target size, but will not split an individual file, therefore if you have files that are larger than the target chunk size, the target size will be exceed. It will also allow some variation in chunk size to meet the objectives described above.

The below screenrecording shows both the command executing and the resulting `WebInterface.html` which allows you to browse the files in the archive and see which chunk each file is stored in.

![demo](https://user-images.githubusercontent.com/39562020/192096471-fcd5b9a4-41c7-4be6-9ad2-7b22111921d0.gif)
 
Background
----------

Project Archiver is a tool to archive folders, compressing them into chunks which can then be uploaded into the cloud. It has been designed to safely archive the data for the various IMI-TRISTAN deliverables, making it easy to transmit data collaborators or archive. It is particularly useful when you have a large (> 1TB) folder containing many (> 1e7) files which you want to archive to an S3 compatible cloud storage system.

