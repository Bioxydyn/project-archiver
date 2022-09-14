# Bioxydyn Project Archiver

[![Coverage Status](https://coveralls.io/repos/github/Bioxydyn/project-archiver/badge.svg?branch=main)](https://coveralls.io/github/Bioxydyn/project-archiver?branch=main)

A tool to archive projects held on a file system, compressing them into chunks which can then be uploaded into the cloud. It has been designed to safely archive the data for the various TRISTAN deliverables, making it easy to transmit data collaborators or archive.

    ./archive -i /path/to/myProject -o /path/to/archive

Will do the following:

1. Scan the input folder and all files in it. For each file will save the relative path, size and last modified date
2. Save out this initial listing to `/path/to/archive/FullListing.txt`
3. Make a decicsion to split which files into chunks
4. Save the decision to `/path/to/archive/ChunkDictionary.yml`. This file will allow a future reader to identify which of the files in FullListing are stored in which chunks, thus allowing them to access them without downloading the full archive
5. Go through each of the chunks and make a zip
    - `/path/to/archive/Chunks/00001/ChunkListing.txt`
    - `/path/to/archive/Chunks/00001/Chunk00001.zip`
6. Read back the zip file and check it contains all expected files and their size is as expected from the full listing
    - `/path/to/archive/Chunks/00001/ChunkCheck.txt`
7. Concat all `ChunkListing.txt` and compare to `FullListing.txt`, create `/path/to/archive/CompleteSuccess.txt` or `CompleteERROR.txt`

Note: 

- Default target chunk size: 2Gb
- Chunk strategy:
   - Prefer to split at a top level
   - Never split files that exist at the same level
   - Print a warning if chunks are > target * 1.5 or < target * 0.5
