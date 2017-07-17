#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
    python setup.py install
else
    pip install -U pip
    pip3 install -U pip3
    python setup.py install
fi
