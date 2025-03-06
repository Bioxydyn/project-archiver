import boto3  # type: ignore
import os
import datetime
import argparse
import sys
from pathlib import Path
from typing import Any


def _get_s3_client() -> Any:
    """
    Get an S3 client using the environment variables.
    """
    # Check if required environment variables are set
    required_env_vars = [
        "ARCHIVER_S3_ACCESS_KEY",
        "ARCHIVER_S3_SECRET_KEY",
        "ARCHIVER_S3_ENDPOINT_URL"
    ]

    missing_vars = []
    for var in required_env_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        print("Error: Missing required environment variables:")
        print(f" {', '.join(missing_vars)}")
        print("Please set these environment variables and try again.")
        sys.exit(1)

    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get("ARCHIVER_S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("ARCHIVER_S3_SECRET_KEY"),
        endpoint_url=os.environ.get("ARCHIVER_S3_ENDPOINT_URL")
    )


def _download_zip_file(
    s3_client: Any,
    key: str,
    output_path: str,
    bucket_name: str = "project-archive"
) -> None:
    s3_client.download_file(
        bucket_name,
        key,
        output_path
    )


def list_all_zip_keys(
    s3_client: Any,
    prefix: str,
    bucket_name: str = "project-archive"
) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])

    # Remove keys which don't end with .zip
    return [key for key in keys if key.endswith(".zip")]


def unzip_into_folder(zip_file_path: str, output_folder: str) -> None:
    os.system(f"unzip -o {zip_file_path} -d {output_folder}")  # noqa: S605


def delete_zip_file(zip_file_path: str) -> None:
    os.remove(zip_file_path)


def ensure_directory_exists(directory_path: str) -> None:
    """
    Ensure that the specified directory exists, creating it if necessary.
    """
    path = Path(directory_path)
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            print(f"Created directory: {directory_path}")
        except Exception as e:
            print(f"Error creating directory {directory_path}: {e}")
            sys.exit(1)
    elif not path.is_dir():
        msg = f"Error: {directory_path} exists but is not a directory"
        print(msg)
        sys.exit(1)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    desc = 'Download and extract all zip files for a project from an S3 bucket.'
    epilog = '''
    Environment variables required:
      ARCHIVER_S3_ACCESS_KEY - S3 access key
      ARCHIVER_S3_SECRET_KEY - S3 secret key
      ARCHIVER_S3_ENDPOINT_URL - S3 endpoint URL

    Example usage:
      python download_all.py --project-name "MyProject" --output-dir "./extracted"
      python download_all.py --project-name "MyProject" --output-dir "./extracted" --bucket-name "custom-bucket"
    '''
    parser = argparse.ArgumentParser(
        description=desc,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--working-dir',
        default='.',
        help='Working directory for temporary zip files (default: current dir)'
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        help='Directory where files will be extracted (will be created if it does not exist)'
    )
    parser.add_argument(
        '--project-name',
        required=True,
        help='Project name prefix for S3 objects to download'
    )
    parser.add_argument(
        '--bucket-name',
        default='project-archive',
        help='S3 bucket name to download from (default: project-archive)'
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    # Ensure directories exist
    ensure_directory_exists(args.working_dir)
    ensure_directory_exists(args.output_dir)

    # Get S3 client (this will check for required environment variables)
    client = _get_s3_client()

    # List all zip keys with the specified project name prefix
    keys = list_all_zip_keys(
        client,
        prefix=args.project_name,
        bucket_name=args.bucket_name
    )
    print(f"Found {len(keys)} zip files to download...")

    if not keys:
        print(f"No zip files found for project '{args.project_name}'")
        print(f"in bucket '{args.bucket_name}'")
        sys.exit(0)

    count = 0
    for key in keys:
        print(f"Downloading {key}...")

        zip_file_path = os.path.join(args.working_dir, key.split("/")[-1])
        _download_zip_file(
            client,
            key=key,
            output_path=zip_file_path,
            bucket_name=args.bucket_name
        )
        unzip_into_folder(zip_file_path, args.output_dir)
        delete_zip_file(zip_file_path)
        timestr: str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        count += 1
        print(f"[{timestr}] Downloaded and unzipped {key}")
        print(f"  ({count:,}/{len(keys):,})")

    print(f"Successfully downloaded and extracted {count} zip files")
    print(f"for project '{args.project_name}'")
