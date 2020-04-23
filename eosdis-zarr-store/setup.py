import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

DEPENDENCIES = [
    'CacheControl>=0.12.6',
    'requests>=2.23.0',
    'requests-futures>=1.0.0',
    # Updates to Zarr to implement the getitems method proposed in https://github.com/zarr-developers/zarr-python/issues/536
    'zarr @ git+ssh://git@github.com/bilts/zarr-python.git@aff8eec3010acbb76746e48a085b93eed67aa799#egg=zarr'
]
DEV_DEPENDENCIES = [
    'setuptools >= 21.0.0',
    'pytest >= 5.1.2',
    'safety >= 1.8.5',
    'coverage >= 4.5.4'
]

setuptools.setup(
    name="eosdis_zarr_store",
    version="0.0.1",
    author="Patrick Quinn",
    author_email="patrick@patrickquinn.net",
    description="Zarr Store class for working with EOSDIS cloud data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.earthdata.nasa.gov/projects/harmony/repos/eosdis-zarr-store",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Private :: Do Not Upload", # Pending open source approval: "License :: OSI Approved :: Apache 2.0 License",
        "Operating System :: OS Independent",

    ],
    python_requires='>=3.6',
    install_requires=DEPENDENCIES,
    extras_require={
        'dev': DEV_DEPENDENCIES # Run `pip install -e .[dev]` to install dev dependencies
    },
)
