# -*- coding: utf-8 -*-
from pathlib import Path
import re

from setuptools import setup


def get_install_requires() -> list:
    return [
        ""
    ]


def get_extras_require() -> dict:
    return {
        "dev": [
            "flake8==4.0.1",
            "flake8-bugbear==21.9.2",
            "flake8-builtins==1.5.3",
            "flake8-fixme==1.1.1",
            "flake8-walrus==1.1.0",
            "flake8-return==1.1.3",
            "flake8-printf-formatting==1.1.2",
            "flake8-broken-line==0.4.0",
            "flake8-comprehensions==3.10.0",
            "flake8-eradicate==1.2.1",
            "flake8-executable==2.1.1",
            "flake8-bandit==3.0.0",
            "flake8-annotations==2.7.0",
            "setuptools",
            "pytest",
            "mypy"
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
