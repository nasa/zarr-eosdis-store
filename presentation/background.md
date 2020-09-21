# EOSDIS Zarr Store: Spatial & Variable Subsetting without Services

## Goal

Produce a library that would let the Zarr Python library read EOSDIS cloud holdings efficiently, without requiring any modifications to our archive.  This has the potential to expand use to new communities and tools, allow more efficient access both in place and outside of the cloud, and therefore save money for the archive as well as time for users.

## Background

This is a demo of a data store I've been working on, building off of the work of a few others.  Adapting the Zarr library (which is meant to read cloud-optimized data stores) read NetCDF4 / HDF 5 files was discussed as a possibility at Summer ESIP last year.  Rich Signell from USGS worked with HDF Group to get [a prototype](https://medium.com/pangeo/cloud-performant-reading-of-netcdf4-hdf5-data-using-the-zarr-library-1a95c5c92314).  The resulting code showed no performance degradation over an equivalent native Zarr store.  This adaptation requies an up-front generation of metadata containing data attributes and byte offsets to allow efficient reads.

## What I did

I recognized that the DMR++ files OPeNDAP / GHRC have started generating on ingest in PI 20.1 contain nearly equivalent information to that required by the Zarr library.  Hearing that small chunk sizes (chunks are a region of data that can / must be read all at once) caused issues for some NetCDF files and required re-chunking (i.e. altering the original data file), I further looked at mitigating that issue to avoid having to re-host data.  In picking through the Zarr code, I came across a for loop that, if changed, would allow a set of optimizations that would greatly improve performance.  I advocated for this in the Zarr tracker and what we need is now being planned.
![png](images/summary.png)

In terms of actual code, I produced a Python library, eosdis-zarr-store that:

1. Implements the Zarr storage API in a natural and familiar way to Zarr developers
2. Sets up HTTP access to allow EDL credential handshaking and, importantly, caching of redirect URLs
3. Adapts our underlying data files and DMR++ files generated on ingest to a Zarr-compatible API
4. Implements optimizations using the API worked out with the Zarr community to make fewer total data reads and do them in parallel where possible

The remainder of this notebook contains results and conclusions.

## How to use it

In the eosdis-zarr-store directory run `pip install -e .`.  Obtain or stage an HDF5 (NetCDF4) file along with a DMR++ file with identical URL + ".dmrpp", you can run "mkdmrpp" ([scripts/mkdmrpp](scripts/mkdmrpp))in this folder to produce DMR++ files.  Then:

```python
from eosdis_zarr_store import Store
import zarr

f = zarr.open(Store(data_file_url))
# Manipulate f as any Zarr store (see examples below)
```

The URLs in this notebook have been redacted, since some produce substantial egress for benchmarking and illustration.  If you need example data, please reach out.

## Helpers and Constants (You can skip this)


```python
# Helpers to draw stuff, generate URLs, and translate bounding boxes to array indices,
# wholly ignoring all the helpful attributes present in the HDF and Zarr metadata
# Please don't judge me on this mess.  It's not called "Clean code fest"

from matplotlib import pyplot as plt
from matplotlib import colors
from ipypb import track
import numpy as np

def show(data, transpose=True):
    plt.rcParams["figure.figsize"] = [16, 8]
    if transpose:
        data = np.transpose(data)
    plt.imshow(data[::-1,:], norm=colors.Normalize(0, 150), cmap='Blues')

def get_aoi(bbox, scale, x0=180, y0=90):
    aoi = (0,
           slice(scale * int(bbox[1] + x0), scale * int(bbox[3] + x0) + 1),
           slice(scale * int(bbox[0] +  y0), scale * int(bbox[2] + y0) + 1))
    shape = [d.stop - d.start for d in aoi[1:]]
    return aoi, shape

url_root = 'https://example.earthdata.nasa.gov/example-staging-url/'
# GPM HHR URLs
filename_template = '3B-HHR.MS.MRG.3IMERG.20051022-S%02d%02d00-E%02d%02d59.%04d.V06B.HDF5'
data_urls = [ url_root + filename_template % (h, m, h, m + 29, h * 60 + m) for h in range(0, 24) for m in range(0, 60, 30) ]

bbox = [10, -100, 47.5, -45]

# Basic file info (also readable from metadata)
GPM_NODATA = -9999.9
gpm_aoi, gpm_shape = get_aoi(bbox, 10)
RSS_NODATA = 251
RSS_SCALE_FACTOR = 0.5 # In-file scale factor is 0.1.  This increases it solely for the purpose of making it show up in pics
rss_aoi, rss_shape = get_aoi(bbox, 4, 360)
rss_aoi = (0, rss_aoi[2], rss_aoi[1])

```

## Problem 1: Atmospheric water vapor off the East Coast on Patrick's wedding day

It rained a little that day in DC and hurricanes were threatening our honeymoon in the Carribbean.

We have a bounding box defined above.  Use data distributed by GHRC derived from the SSMIS sensor of the F16 DMSP satellite to build a picture.

### Without Partial Access

Download 2.6 MB file and subset it


```python
%%time
from h5py import File as H5File
import requests
from io import BytesIO

response = requests.get(url_root + 'f16_ssmis_20051022v7.nc')
with H5File(BytesIO(response.content), 'r') as f:
    aoi_data = f['atmosphere_water_vapor_content'][rss_aoi]
    show(np.where(aoi_data < RSS_NODATA, aoi_data * RSS_SCALE_FACTOR, 0), transpose=False)

```

    CPU times: user 206 ms, sys: 43.1 ms, total: 249 ms
    Wall time: 8.32 s



![png](images/output_3_1.png)


### Partial Access Step 1 - Make our data readable in Zarr for partial access

Mimick a Zarr store by reading OPeNDAP's DMR++ files and returning their metadata in a Zarr interface.  DMR++ files are planned to be generated on ingest

Downloads 200 KB of data from the 2.6 MB file with conventional Zarr storage implementation.

Result: 12 data requests, each of which goes through internet services, pre-signs a URL, and redirects to the data range.  All sequentially.  Slow.


```python
%%time
from unoptimized_zarr_store import Store
import zarr

f = zarr.open(Store(url_root + 'f16_ssmis_20051022v7.nc'))
aoi_data = f['atmosphere_water_vapor_content'][rss_aoi]
show(np.where(aoi_data < RSS_NODATA, aoi_data * RSS_SCALE_FACTOR, 0), transpose=False)
```

    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1455912:1465233] (9321 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1465240:1472085] (6845 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1472088:1479515] (7427 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1479520:1488808] (9288 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1562808:1564995] (2187 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1565000:1572245] (7245 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1572248:1582809] (10561 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1582816:1593902] (11086 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1635624:1636392] (768 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1636392:1639400] (3008 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1639400:1645720] (6320 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1645720:1654949] (9229 bytes)
    CPU times: user 246 ms, sys: 45.5 ms, total: 292 ms
    Wall time: 39.3 s



![png](images/output_5_1.png)


### Partial Access Step 2 - Make Zarr reads fast

Downloads 200 KB of data from the 2.6 MB file with Zarr optimizations:
1. (Working with Zarr community) Implement "getitems" concept, allowing storage to know all of the chunks that will be accessed up front
2. Combine nearby range requests into single HTTP requests before sending them, allowing fewer requests.
3. Cache presigned URLs returned by the archive for a short time, as directed by caching headers (TEA has a ticket to add these), allowing reuse and avoiding many round-trips and redirects
4. Run the first data range request serially to get the presigned URL.  Run subsequent requests in parallel.

Result: 3 data requests, one of which goes through internet services, pre-signs a URL, and redirects to the data range.  The following two reuse the signed URL and fetch in parallel.  Faster!

When more than a couple of chunks are involved, this is expected to be faster than the native Zarr S3 format is capable of, and the more chunks involved in a read the more it improves.


```python
%%time
from eosdis_zarr_store import Store
import zarr

f = zarr.open(Store(url_root + 'f16_ssmis_20051022v7.nc'))
aoi_data = f['atmosphere_water_vapor_content'][rss_aoi]
show(np.where(aoi_data < RSS_NODATA, aoi_data * RSS_SCALE_FACTOR, 0), transpose=False)
```

    Starting new range due to gap of 74000 bytes
    Starting new range due to gap of 41722 bytes
    Merged 12 requests into 3
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1455912:1488808] (32896 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1562808:1593902] (31094 bytes)
    Reading https://example.earthdata.nasa.gov/example-staging-url/f16_ssmis_20051022v7.nc [1635624:1654949] (19325 bytes)
    CPU times: user 137 ms, sys: 19.9 ms, total: 156 ms
    Wall time: 17.3 s



![png](images/output_7_1.png)


## Problem 2: Rain along the East Coast on Patrick's wedding day

With the same bounding box above, get half-hourly high-quality precipitation values from GPM and sum them for the entire day (48 L3 global data files)

### Without Partial Access

Download approximately 500MB of data in whole files and processes them


```python
%%time
from h5py import File as H5File
import requests
from io import BytesIO

result = np.zeros(gpm_shape)
for url in track(data_urls):
    response = requests.get(url)
    with H5File(BytesIO(response.content), 'r') as f:
        aoi_data = f['Grid/HQprecipitation'][gpm_aoi]
        result = result + np.where(aoi_data != GPM_NODATA, aoi_data / 2, 0)
show(result)
```


<div><span class="Text-label" style="display:inline-block; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; min-width:0; max-width:15ex; vertical-align:middle; text-align:right"></span>
<progress style="width:60ex" max="48" value="48" class="Progress-main"/></progress>
<span class="Progress-label"><strong>100%</strong></span>
<span class="Iteration-label">48/48</span>
<span class="Time-label">[05:55<00:06, 7.39s/it]</span></div>


    CPU times: user 8.51 s, sys: 3 s, total: 11.5 s
    Wall time: 5min 54s



![png](images/output_9_2.png)


### With the EOSDIS Zarr Store

Downloads approximately 5 MB of data by doing partial reads in fewer lines of code


```python
%%time
from eosdis_zarr_store import Store
import zarr

result = np.zeros(gpm_shape)
for url in track(data_urls):
    f = zarr.open(Store(url, quiet=True))
    aoi_data = f['Grid/HQprecipitation'][gpm_aoi]
    result = result + np.where(aoi_data != GPM_NODATA, aoi_data / 2, 0)
show(result)
```


<div><span class="Text-label" style="display:inline-block; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; min-width:0; max-width:15ex; vertical-align:middle; text-align:right"></span>
<progress style="width:60ex" max="48" value="48" class="Progress-main"/></progress>
<span class="Progress-label"><strong>100%</strong></span>
<span class="Iteration-label">48/48</span>
<span class="Time-label">[03:58<00:05, 4.96s/it]</span></div>


    CPU times: user 1.72 s, sys: 185 ms, total: 1.91 s
    Wall time: 3min 57s



![png](images/output_12_2.png)


### Comparison to L3 Daily Average Product

Download a 30 MB file from the daily average collection to produce a similar result, validating the result at 6x egress cost of partial access for all of the half-hourly source files.


```python
%%time
from h5py import File as H5File
import requests
from io import BytesIO

response = requests.get(url_root + '3B-DAY.MS.MRG.3IMERG.20051022-S000000-E235959.V06.nc4')
with H5File(BytesIO(response.content), 'r') as f:
    show(f['HQprecipitation'][gpm_aoi])
```

    CPU times: user 415 ms, sys: 187 ms, total: 602 ms
    Wall time: 11.2 s



![png](images/output_15_1.png)


## I can see my house from here!

Download GEDI L2B data.  Use small geolocation arrays to find the area of interest, then download only the data within those chunks.

A full file download is 1.3 GB.  The code below downloads approximately 15 MB of data and metadata.  This reduces a 15 minute download to about 8s.  (Aside: the download is 2/3 metadata, which could be dramatically reduced by using Zarr's default format rather than DMR++)


```python
from eosdis_zarr_store import Store
import zarr
import numpy as np

url = 'https://example.earthdata.nasa.gov/example-staging-url/GEDI02_B_2019182140038_O03117_T05635_02_001_01.h5'
f = zarr.open(Store(url))
print(f.tree())
```

    /
     ├── BEAM0000
     │   ├── algorithmrun_flag (238914,) uint8
     │   ├── ancillary
     │   │   ├── dz (1,) float64
     │   │   ├── maxheight_cuttoff (1,) float64
     │   │   ├── rg_eg_constraint_center_buffer (1,) int32
     │   │   ├── rg_eg_mpfit_max_func_evals (1,) uint16
     │   │   ├── rg_eg_mpfit_maxiters (1,) uint16
     │   │   ├── rg_eg_mpfit_tolerance (1,) float64
     │   │   ├── signal_search_buff (1,) float64
     │   │   └── tx_noise_stddev_multiplier (1,) float64
     │   ├── beam (238914,) uint16
     │   ├── channel (238914,) uint8
     │   ├── cover (238914,) float32
     │   ├── cover_z (238914, 30) float32
     │   ├── fhd_normal (238914,) float32
     │   ├── geolocation
     │   │   ├── degrade_flag (238914,) int16
     │   │   ├── delta_time (238914,) float64
     │   │   ├── digital_elevation_model (238914,) float32
     │   │   ├── elev_highestreturn (238914,) float32
     │   │   ├── elev_lowestmode (238914,) float32
     │   │   ├── elevation_bin0 (238914,) float64
     │   │   ├── elevation_bin0_error (238914,) float32
     │   │   ├── elevation_lastbin (238914,) float64
     │   │   ├── elevation_lastbin_error (238914,) float32
     │   │   ├── height_bin0 (238914,) float32
     │   │   ├── height_lastbin (238914,) float32
     │   │   ├── lat_highestreturn (238914,) float64
     │   │   ├── lat_lowestmode (238914,) float64
     │   │   ├── latitude_bin0 (238914,) float64
     │   │   ├── latitude_bin0_error (238914,) float32
     │   │   ├── latitude_lastbin (238914,) float64
     │   │   ├── latitude_lastbin_error (238914,) float32
     │   │   ├── local_beam_azimuth (238914,) float32
     │   │   ├── local_beam_elevation (238914,) float32
     │   │   ├── lon_highestreturn (238914,) float64
     │   │   ├── lon_lowestmode (238914,) float64
     │   │   ├── longitude_bin0 (238914,) float64
     │   │   ├── longitude_bin0_error (238914,) float32
     │   │   ├── longitude_lastbin (238914,) float64
     │   │   ├── longitude_lastbin_error (238914,) float32
     │   │   ├── solar_azimuth (238914,) float32
     │   │   └── solar_elevation (238914,) float32
     │   ├── l2a_quality_flag (238914,) uint8
     │   ├── l2b_quality_flag (238914,) uint8
     │   ├── land_cover_data
     │   │   ├── landsat_treecover (238914,) float64
     │   │   ├── modis_nonvegetated (238914,) float64
     │   │   ├── modis_nonvegetated_sd (238914,) float64
     │   │   ├── modis_treecover (238914,) float64
     │   │   └── modis_treecover_sd (238914,) float64
     │   ├── master_frac (238914,) float64
     │   ├── master_int (238914,) uint32
     │   ├── num_detectedmodes (238914,) uint8
     │   ├── omega (238914,) float32
     │   ├── pai (238914,) float32
     │   ├── pai_z (238914, 30) float32
     │   ├── pavd_z (238914, 30) float32
     │   ├── pgap_theta (238914,) float32
     │   ├── pgap_theta_error (238914,) float32
     │   ├── pgap_theta_z (7926559,) float32
     │   ├── rg (238914,) float32
     │   ├── rh100 (238914,) int16
     │   ├── rhog (238914,) float32
     │   ├── rhog_error (238914,) float32
     │   ├── rhov (238914,) float32
     │   ├── rhov_error (238914,) float32
     │   ├── rossg (238914,) float32
     │   ├── rv (238914,) float32
     │   ├── rx_processing
     │   │   ├── algorithmrun_flag_a1 (238914,) uint8
     │   │   ├── algorithmrun_flag_a2 (238914,) uint8
     │   │   ├── algorithmrun_flag_a3 (238914,) uint8
     │   │   ├── algorithmrun_flag_a4 (238914,) uint8
     │   │   ├── algorithmrun_flag_a5 (238914,) uint8
     │   │   ├── algorithmrun_flag_a6 (238914,) uint8
     │   │   ├── pgap_theta_a1 (238914,) float32
     │   │   ├── pgap_theta_a2 (238914,) float32
     │   │   ├── pgap_theta_a3 (238914,) float32
     │   │   ├── pgap_theta_a4 (238914,) float32
     │   │   ├── pgap_theta_a5 (238914,) float32
     │   │   ├── pgap_theta_a6 (238914,) float32
     │   │   ├── pgap_theta_error_a1 (238914,) float32
     │   │   ├── pgap_theta_error_a2 (238914,) float32
     │   │   ├── pgap_theta_error_a3 (238914,) float32
     │   │   ├── pgap_theta_error_a4 (238914,) float32
     │   │   ├── pgap_theta_error_a5 (238914,) float32
     │   │   ├── pgap_theta_error_a6 (238914,) float32
     │   │   ├── rg_a1 (238914,) float32
     │   │   ├── rg_a2 (238914,) float32
     │   │   ├── rg_a3 (238914,) float32
     │   │   ├── rg_a4 (238914,) float32
     │   │   ├── rg_a5 (238914,) float32
     │   │   ├── rg_a6 (238914,) float32
     │   │   ├── rg_eg_amplitude_a1 (238914,) float32
     │   │   ├── rg_eg_amplitude_a2 (238914,) float32
     │   │   ├── rg_eg_amplitude_a3 (238914,) float32
     │   │   ├── rg_eg_amplitude_a4 (238914,) float32
     │   │   ├── rg_eg_amplitude_a5 (238914,) float32
     │   │   ├── rg_eg_amplitude_a6 (238914,) float32
     │   │   ├── rg_eg_amplitude_error_a1 (238914,) float32
     │   │   ├── rg_eg_amplitude_error_a2 (238914,) float32
     │   │   ├── rg_eg_amplitude_error_a3 (238914,) float32
     │   │   ├── rg_eg_amplitude_error_a4 (238914,) float32
     │   │   ├── rg_eg_amplitude_error_a5 (238914,) float32
     │   │   ├── rg_eg_amplitude_error_a6 (238914,) float32
     │   │   ├── rg_eg_center_a1 (238914,) float32
     │   │   ├── rg_eg_center_a2 (238914,) float32
     │   │   ├── rg_eg_center_a3 (238914,) float32
     │   │   ├── rg_eg_center_a4 (238914,) float32
     │   │   ├── rg_eg_center_a5 (238914,) float32
     │   │   ├── rg_eg_center_a6 (238914,) float32
     │   │   ├── rg_eg_center_error_a1 (238914,) float32
     │   │   ├── rg_eg_center_error_a2 (238914,) float32
     │   │   ├── rg_eg_center_error_a3 (238914,) float32
     │   │   ├── rg_eg_center_error_a4 (238914,) float32
     │   │   ├── rg_eg_center_error_a5 (238914,) float32
     │   │   ├── rg_eg_center_error_a6 (238914,) float32
     │   │   ├── rg_eg_chisq_a1 (238914,) float32
     │   │   ├── rg_eg_chisq_a2 (238914,) float32
     │   │   ├── rg_eg_chisq_a3 (238914,) float32
     │   │   ├── rg_eg_chisq_a4 (238914,) float32
     │   │   ├── rg_eg_chisq_a5 (238914,) float32
     │   │   ├── rg_eg_chisq_a6 (238914,) float32
     │   │   ├── rg_eg_flag_a1 (238914,) int16
     │   │   ├── rg_eg_flag_a2 (238914,) int16
     │   │   ├── rg_eg_flag_a3 (238914,) int16
     │   │   ├── rg_eg_flag_a4 (238914,) int16
     │   │   ├── rg_eg_flag_a5 (238914,) int16
     │   │   ├── rg_eg_flag_a6 (238914,) int16
     │   │   ├── rg_eg_gamma_a1 (238914,) float32
     │   │   ├── rg_eg_gamma_a2 (238914,) float32
     │   │   ├── rg_eg_gamma_a3 (238914,) float32
     │   │   ├── rg_eg_gamma_a4 (238914,) float32
     │   │   ├── rg_eg_gamma_a5 (238914,) float32
     │   │   ├── rg_eg_gamma_a6 (238914,) float32
     │   │   ├── rg_eg_gamma_error_a1 (238914,) float32
     │   │   ├── rg_eg_gamma_error_a2 (238914,) float32
     │   │   ├── rg_eg_gamma_error_a3 (238914,) float32
     │   │   ├── rg_eg_gamma_error_a4 (238914,) float32
     │   │   ├── rg_eg_gamma_error_a5 (238914,) float32
     │   │   ├── rg_eg_gamma_error_a6 (238914,) float32
     │   │   ├── rg_eg_niter_a1 (238914,) uint8
     │   │   ├── rg_eg_niter_a2 (238914,) uint8
     │   │   ├── rg_eg_niter_a3 (238914,) uint8
     │   │   ├── rg_eg_niter_a4 (238914,) uint8
     │   │   ├── rg_eg_niter_a5 (238914,) uint8
     │   │   ├── rg_eg_niter_a6 (238914,) uint8
     │   │   ├── rg_eg_sigma_a1 (238914,) float32
     │   │   ├── rg_eg_sigma_a2 (238914,) float32
     │   │   ├── rg_eg_sigma_a3 (238914,) float32
     │   │   ├── rg_eg_sigma_a4 (238914,) float32
     │   │   ├── rg_eg_sigma_a5 (238914,) float32
     │   │   ├── rg_eg_sigma_a6 (238914,) float32
     │   │   ├── rg_eg_sigma_error_a1 (238914,) float32
     │   │   ├── rg_eg_sigma_error_a2 (238914,) float32
     │   │   ├── rg_eg_sigma_error_a3 (238914,) float32
     │   │   ├── rg_eg_sigma_error_a4 (238914,) float32
     │   │   ├── rg_eg_sigma_error_a5 (238914,) float32
     │   │   ├── rg_eg_sigma_error_a6 (238914,) float32
     │   │   ├── rg_error_a1 (238914,) float32
     │   │   ├── rg_error_a2 (238914,) float32
     │   │   ├── rg_error_a3 (238914,) float32
     │   │   ├── rg_error_a4 (238914,) float32
     │   │   ├── rg_error_a5 (238914,) float32
     │   │   ├── rg_error_a6 (238914,) float32
     │   │   ├── rv_a1 (238914,) float32
     │   │   ├── rv_a2 (238914,) float32
     │   │   ├── rv_a3 (238914,) float32
     │   │   ├── rv_a4 (238914,) float32
     │   │   ├── rv_a5 (238914,) float32
     │   │   ├── rv_a6 (238914,) float32
     │   │   ├── rx_energy_a1 (238914,) float32
     │   │   ├── rx_energy_a2 (238914,) float32
     │   │   ├── rx_energy_a3 (238914,) float32
     │   │   ├── rx_energy_a4 (238914,) float32
     │   │   ├── rx_energy_a5 (238914,) float32
     │   │   └── rx_energy_a6 (238914,) float32
     │   ├── rx_range_highestreturn (238914,) float64
     │   ├── selected_l2a_algorithm (238914,) uint8
     │   ├── selected_rg_algorithm (238914,) uint8
     │   ├── sensitivity (238914,) float32
     │   ├── stale_return_flag (238914,) uint8
     │   └── surface_flag (238914,) uint8
     ==== 1000 lines removed for brevity, see commit history ====
     └── METADATA



```python
n, w, s, e = [40.2, -75.25, 40.15, -75.2]

geoloc = f['BEAM0000/geolocation']
all_lats = geoloc['latitude_bin0'][:]
all_lons = geoloc['longitude_bin0'][:]
valid_lat_i = np.where(np.logical_and(all_lats >= s, all_lats <= n))
valid_lon_i = np.where(np.logical_and(all_lons >= w, all_lons <= e))
indices = np.intersect1d(valid_lat_i, valid_lon_i)

lats = all_lats[indices]
lons = all_lons[indices]
data = f['BEAM0000/cover'][:][indices]
data_i = np.where(data != -9999)
data = data[data_i]
lats = lats[data_i]
lons = lons[data_i]

ambler = plt.imread('ambler.png')
fig, ax = plt.subplots(figsize=(10,10))
ax.scatter(lons, lats, s=50, c=data, cmap='Greens')
ax.set_xlim(w, e)
ax.set_ylim(s, n)
ax.imshow(ambler, zorder=0, extent = [w, e, s, n], aspect='equal')
```

    Merged 17 requests into 1
    Reading https://example.earthdata.nasa.gov/example-staging-url/GEDI02_B_2019182140038_O03117_T05635_02_001_01.h5 [35434732:36859543] (1424811 bytes)
    Merged 17 requests into 1
    Reading https://example.earthdata.nasa.gov/example-staging-url/GEDI02_B_2019182140038_O03117_T05635_02_001_01.h5 [43931476:45340903] (1409427 bytes)
    Merged 17 requests into 1
    Reading https://example.earthdata.nasa.gov/example-staging-url/GEDI02_B_2019182140038_O03117_T05635_02_001_01.h5 [12035:442316] (430281 bytes)





    <matplotlib.image.AxesImage at 0x11f1a7eb0>




![png](images/output_18_2.png)


## Why is it sometimes slower?

![png](images/request-overhead.png)

We pay a penalty for every new file we access, needing to go over the Internet, through the Internet services stack, the request signing process, and ultimately get redirected to S3.  The Zarr store has to pay this penalty twice to read the metadata and then the file, while a full-file download only pays the penalty once.  With current performance, the break-even point in file size is about 10 MB.  That is to say, if a user wants to access even a tiny amount of data in each granule from a collection whose granules are under 10 MB in size, he or she is better off downloading the granules.  While there is some uncontrollable overhead, there is significant room for improvement in areas that are under our control to promote inexpensive access patterns while improving time to science.

## Conclusions

* If providers generate DMR++ on ingest, we can expose our data efficiently using a Python API that is gaining increasing traction, particulary in the Pangeo community, with minimal storage overhead
* Works out of the cloud, but works even better / faster in the cloud for analysis near data
* For partial access cases, an overall egress reduction of 90% or more could be possible, as demonstrated
* Chunking matters.  This work makes smaller chunks more desirable, which has not historically been the case with Zarr
* Overhead in our stack, from EDL, to Internet services, to redirects, are eating up the potential user savings.  At a 90% egress reduction, we struggle to compete with "Just download everything."  How do we balance preventing undesirable behavior with encouraging desirable behavior?
* There are lingering questions about whether DMR++ is the correct format to capture this metadata in.  Zarr's native format is in many cases more complete and easier to parse while having mechanisms for more easily working with the 100,000-ish chunks in GEDI granules and for unifying multiple granules into a coherent view.

## Limitations / Needs

* The DMR++ file must be generated on ingest into the cloud, which is currently optional
* Only works on HDF5 and NetCDF4 files.  In principle, it could work on HDF4 / NetCDF Classic files but nothing yet generates the necessary metadata
* DMR++ does not quite specify everything we could need for some datasets.  We assume little endian byte order and column-major ordering.

## Future Work

* Packaging, unit tests, and docs sufficient for publication
* Open source (relies on a naming decision)
* Cache repeated calls for the same byte ranges to avoid requerying data we have
* Implement unknown / undocumented areas of the DMR++ spec, including compression types and data filters
* Tests with Dask and XArray
* Implement CF conventions to populate fill values, offsets, scales, etc
* Extensions to present L3 global collections as a coherent data cube

I strongly believe in this access pattern as a win for our users and ourselves.  To the extent it is not fully realized, it suffers from being an early adopter of our cloud access stack.  My sincere hope is that we can learn from it to improve partial file access not only here but in other tools and libraries.
