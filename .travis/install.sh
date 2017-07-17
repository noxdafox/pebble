#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
    python setup.py install
else
    pip install -U pip
    python setup.py install
fi
