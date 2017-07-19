#!/bin/bash

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
    brew update

    case "${TOXENV}" in
        py2)
            brew install python
            virtualenv venv -p python
            ;;
        py3)
            brew install python3
            virtualenv venv -p python3
            ;;
    esac

    source venv/bin/activate
    sudo pip install --upgrade pip
    sudo pip install --upgrade setuptools
    sudo pip install --upgrade pytest
    python setup.py install

    sudo find / -name "py.test"
    sudo find / -name "pytest"
else
    sudo pip install --upgrade pip
    sudo pip install --upgrade setuptools
    sudo pip install --upgrade pytest
    python setup.py install
fi
