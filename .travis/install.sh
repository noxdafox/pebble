#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
    python setup.py install
else
    sudo apt-get update -qq
    sudo apt-get install -qq python-setuptools python3-setuptools
    python setup.py install
fi
