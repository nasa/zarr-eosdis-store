# Aszarr Library

Library for doing performance partial reads of remote HDF and NetCDF files using the [Zarr](https://zarr.readthedocs.io/en/stable/) library.

## Installation

Aszarr requires ... and is ... It is published to ... To install:

```bash
$ pip install zarr-eosdis-store

# install specific version
$ pip install zarr-eosdis-store==0.1.0

# in a requirements.txt file - the tilda (~) will install the highest compatible version
zarr-eosdis-store~=0.1.0
```

## Modules

| Module   | Description |
| -------- | ----------- |
| common   | Internal profiling functions and async HTTP session |
| dmrpp    | Functions for converting a DMR++ file to Zarr metadata |
| stores   | Module containing classes for Zarr stores |
| version  | Contains __version__ |

### common

### dmrpp

### reader

#### store

## Usage

TODO
- DMRPP to Zarr
- Opening EOSDIS HDF5 file as Zarr store
- new Jupyter notebook tutorial


## About
zarr-eosdis-store is ... {TODO - link to parent org and funding agency}