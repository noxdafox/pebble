import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="Pebble",
    version="1.1.5",
    author="Matteo Cafasso",
    author_email="noxdafox@gmail.com",
    description=("Threading and multiprocessing eye-candy."),
    license="LGPL",
    keywords="thread process pool decorator",
    url="https://github.com/noxdafox/pebble",
    packages=['pebble', 'tests'],
    long_description=read('README.txt'),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)"
    ],
)
