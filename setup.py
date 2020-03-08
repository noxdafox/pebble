import os
import subprocess
from setuptools import setup, find_packages


CWD = os.path.dirname(__file__)


def read(fname):
    return open(os.path.join(CWD, fname)).read()


def package_version():
    """Get the package version via Git Tag."""
    version_path = os.path.join(CWD, 'version.py')
    init_path = os.path.join(CWD, 'pebble', '__init__.py')

    version = read_version(version_path)
    write_version(version_path, version)
    write_version(init_path, version, mode='a')

    return version


def read_version(path):
    try:
        return subprocess.check_output(('git', 'describe')).rstrip().decode()
    except Exception:
        with open(path) as version_file:
            version_string = version_file.read().split('=')[-1]
            return version_string.strip().replace('"', '')


def write_version(path, version, mode='w'):
    msg = '"""Versioning controlled via Git Tag, check setup.py"""'
    with open(path, mode) as version_file:
        version_file.write(msg + os.linesep + os.linesep +
                           '__version__ = "{}"'.format(version) +
                           os.linesep)


setup(
    name="Pebble",
    version="{}".format(package_version()),
    author="Matteo Cafasso",
    author_email="noxdafox@gmail.com",
    description=("Threading and multiprocessing eye-candy."),
    license="LGPL",
    keywords="thread process pool decorator",
    url="https://github.com/noxdafox/pebble",
    packages=find_packages(exclude=["tests"]),
    extras_require={":python_version<'3'": ["futures"]},
    long_description=read('README.rst'),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: " +
        "GNU Library or Lesser General Public License (LGPL)"
    ],
)
