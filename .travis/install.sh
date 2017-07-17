#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
    python setup.py install
else
    pip install -U pip
    pip install -U setuptools
    python setup.py install
fi
