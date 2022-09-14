#!/bin/bash

source venv/bin/activate

flake8 archiver --exclude=*/cpp/*,*/external/*,.svn,CVS,.bzr,.hg,.git,__pycache__,.tox,.eggs,*.egg --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 tests --count --select=E9,F63,F7,F82 --show-source --statistics

flake8 archiver --exclude=*/cpp/*,*/external/*,.svn,CVS,.bzr,.hg,.git,__pycache__,.tox,.eggs,*.egg --count --max-complexity=60 --ignore=E203,W503,ANN101,ANN204,S101 --max-line-length=120 --statistics
flake8 tests --count --max-complexity=60 --ignore=E203,W503,ANN101,ANN204,S101 --max-line-length=120 --statistics

mypy archiver
mypy tests

pytest tests
