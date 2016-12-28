import os
import sys
from setuptools import setup, find_packages


required_packages = []

# Python < 3 requires backported futures package
if sys.version_info.major < 3:
    required_packages.append('futures')


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="Pebble",
    version="4.1.0",
    author="Matteo Cafasso",
    author_email="noxdafox@gmail.com",
    description=("Threading and multiprocessing eye-candy."),
    license="LGPL",
    keywords="thread process pool decorator",
    url="https://github.com/noxdafox/pebble",
    packages=find_packages(exclude=["tests"]),
    install_requires=required_packages,
    long_description=read('README.rst'),
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
