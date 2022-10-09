import os
import fileinput
from setuptools import setup, find_packages


CWD = os.path.dirname(__file__)


def package_version():
    module_path = os.path.join(CWD, 'pebble', '__init__.py')
    for line in fileinput.input(module_path):
        if line.startswith('__version__'):
            return line.split('=')[-1].strip().replace('\'', '')


setup(
    name="Pebble",
    version=package_version(),
    author="Matteo Cafasso",
    author_email="noxdafox@gmail.com",
    description=("Threading and multiprocessing eye-candy."),
    license="LGPL",
    keywords="thread process pool decorator",
    url="https://github.com/noxdafox/pebble",
    packages=find_packages(exclude=["test"]),
    long_description=open(os.path.join(CWD, 'README.rst')).read(),
    python_requires=">=3.6",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: " +
        "GNU Library or Lesser General Public License (LGPL)"
    ],
)
