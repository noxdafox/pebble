#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
    brew update
    brew install python3
    virtualenv venv -p python3
    source venv/bin/activate
    python setup.py install
else
    sudo pip install --upgrade pip
    sudo pip install --upgrade setuptools
    python setup.py install
fi
