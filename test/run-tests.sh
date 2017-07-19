#!/bin/bash

# This script runs the tests singularly to overcome Python 3
# multiprocessing Process start methods limitations

set -e

for testfile in $(find test/ -name "test_*.py")
do
    python -m pytest $testfile -v
done
