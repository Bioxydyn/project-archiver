# -*- coding: utf-8 -*-
from pathlib import Path
import re

from setuptools import setup


def get_install_requires() -> list:
    return [
        "setuptools"
    ]


def get_extras_require() -> dict:
    return {
        "dev": [
            "flake8",
            "flake8-bugbear",
            "flake8-builtins",
            "flake8-fixme",
            "flake8-walrus",
            "flake8-return",
            "flake8-printf-formatting",
            "flake8-broken-line",
            "flake8-comprehensions",
            "flake8-eradicate",
            "flake8-executable",
            "flake8-bandit",
            "flake8-annotations",
            "setuptools",
            "pytest",
            "mypy",
            "coverage"
        ]
    }


def get_version(package: str) -> str:
    """
    Return package version as listed in `__version__` in `vault/version.py`.
    """
    version = Path(package, "version.py").read_text()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", version).group(1)


setup(
    name="archiver",
    version=get_version("archiver"),
    author="Bioxydyn Limited",
    author_email="matthew.heaton@bioxydyn.com",
    description="Tool to archive folders and files",
    long_description="",
    include_package_data=True,
    packages=['archiver'],
    zip_safe=False,
    install_requires=get_install_requires(),
    extras_require=get_extras_require(),
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "archiver = archiver.cli:cli",
        ]
    }
)
