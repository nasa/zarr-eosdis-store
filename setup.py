#!/usr/bin/env python
from setuptools import setup, find_packages
from imp import load_source
from os import path
import io

# use README for setup desciption
with open("README.rst", "r") as f:
    long_description = f.read()

# get version of package
__version__ = load_source('eosdis_store.version', 'eosdis_store/version.py').__version__

# get the dependencies and installs
with io.open(path.join(path.abspath(path.dirname(__file__)), 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

# remove direct installs from github
install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if 'git+' in x]

# get dev dependencies
with io.open(path.join(path.abspath(path.dirname(__file__)), 'requirements-dev.txt'), encoding='utf-8') as f:
    dev_reqs = f.read().split('\n')


setup(
    name="zarr-eosdis-store",
    version=__version__,
    author="Patrick Quinn, Matthew Hanson",
    author_email="patrick@patrickquinn.net",
    description="Zarr Store class for working with EOSDIS cloud data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nasa/zarr-eosdis-store",
    packages=find_packages(exclude=['docs', 'tests*']),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
    ],
    python_requires='>=3.8',
    install_requires=install_requires,
    dependency_links=dependency_links,
    extras_require={
        'dev': dev_reqs # Run `pip install -e .[dev]` to install dev dependencies
    },
)
