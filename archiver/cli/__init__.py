import sys

from archiver.archiver import ArchiveRunner


def cli() -> int:
    runner = ArchiveRunner()
    runner.parse_arguments(sys.argv[1:])
    runner.run()
    return 0
