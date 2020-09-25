import json
import os
import unittest

import numpy as np
import requests
import xml.etree.ElementTree as ElementTree
import xarray
import zarr

from eosdis_store import EosdisStore, ConsolidatedChunkStore

testpath = os.path.dirname(__file__)

fixtures = [
    {
        "url": 'https://harmony.uat.earthdata.nasa.gov/service-results/harmony-uat-staging/public/demo/zarr-store/f16_ssmis_20051022v7.nc',
        "aoi": (0, slice(400, 549, None), slice(1040, 1261, None))
    },
    {
        "url": "https://harmony.uat.earthdata.nasa.gov/service-results/harmony-uat-staging/public/demo/zarr-store/3B-HHR.MS.MRG.3IMERG.20051022-S000000-E002959.0000.V06B.HDF5",
        "aoi": (0, slice(800, 1351, None), slice(1000, 1371, None))
    },
    {
        "url": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-protected/MODIS_A-JPL-L2P-v2019.0/20200911000001-JPL-L2P_GHRSST-SSTskin-MODIS_A-N-v02.0-fv01.0.nc",
        "aoi": (0, slice(1800, 2000, None), slice(100, 400, None))
    },
    {
        "url": "https://archive.podaac.uat.earthdata.nasa.gov/podaac-uat-cumulus-protected/MODIS_A-JPL-L2P-v2019.0/20200911004001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
        "aoi": (0, slice(1800, 2000, None), slice(100, 400, None))
    }
]


def open_eosdis_store(idx=0):
    return EosdisStore(fixtures[idx]["url"], fixtures[idx]["url"] + ".dmrpp")


class TestZarr(unittest.TestCase):

    @classmethod
    def get_test_xml(cls, idx=0):
        url = fixtures[idx]["url"]
        dmrpp = requests.get(url + '.dmrpp').text
        tree = ElementTree.fromstring(dmrpp)
        return tree

    def test_eosdis_store_metadata(self):
        store = open_eosdis_store()
        keys = list(store.keys())
        assert(len(keys) == 26)
        # test default dmr_url
        store = EosdisStore(fixtures[0]["url"])
        keys = list(store.keys())
        assert(len(keys) == 26)

    def test_eosdis_store_open(self):
        store = zarr.open(open_eosdis_store())
        arrays = list(store.arrays())
        assert(len(arrays) == 8)
        assert(arrays[0][0] == 'atmosphere_cloud_liquid_water_content')
        arr = arrays[0][1]
        assert(type(arr) == zarr.core.Array)
        assert(arr.name == '/atmosphere_cloud_liquid_water_content')       
        assert(arr.shape == (2, 720, 1440))

    def test_eosdis_store_read(self):
        store = zarr.open(open_eosdis_store())
        arr = store['wind_speed'][fixtures[0]["aoi"]]
        assert(arr.shape == (149, 221))
        assert(arr[0][0] == 19)
        assert(arr.mean() == 169.29050381123022)

    def test_eosdis_store_getranges_combined(self):
        store = open_eosdis_store()
        ranges = [
            ('wind_speed/0.4.11', 768280, 6830),
            ('wind_speed/0.4.12', 775112, 5759)
        ]
        result = store._getranges(ranges)
        assert(len(result) == 2)
        assert(len(store.responses) == 1)

    def test_eosdis_store_getranges_split(self):
        store = open_eosdis_store()
        ranges = [
            ('wind_speed/0.4.11', 768280, 6830),
            ('wind_speed/0.4.12', 785112, 5759)
        ]
        result = store._getranges(ranges)
        assert(len(result) == 2)
        assert(len(store.responses) == 2)

    def test_eosdis_store_parallel_reads(self):
        store = zarr.open(open_eosdis_store())
        arr = store['wind_speed'][fixtures[0]["aoi"]]
        responses = store.store.responses
        end_time = responses[0].start + responses[1].elapsed
        for r in responses[1:]:
            assert(r.start < end_time)


class TestXArray(unittest.TestCase):

    @classmethod
    def _setUpClass(cls):
        store = open_eosdis_store(2)
        # use patched zarr metadata file as workaround to incorrect DMR++ files showing Int8 datasets as Int16
        fix = fixtures[2]
        bname = f"{os.path.splitext(os.path.basename(fix['url']))[0]}.zarr.json"
        with open(os.path.join(testpath, 'fixtures', bname)) as f:
            meta = json.loads(f.read())

        store = ConsolidatedChunkStore(meta, fix["url"])

        cls.xa_noscale = xarray.open_zarr(store, mask_and_scale=False)
        cls.xa = xarray.open_zarr(store, mask_and_scale=True)

    def test_scale_offset(self):
        store = open_eosdis_store(0)
        var = 'wind_speed'

        xa_noscale = xarray.open_zarr(store, mask_and_scale=False)
        xa = xarray.open_zarr(store, mask_and_scale=True)

        # get values without scale and offset
        wv = xa_noscale[var]
        assert(hasattr(wv, "scale_factor"))
        assert(hasattr(wv, "add_offset"))
        arr = wv[fixtures[0]["aoi"]]
        mean = arr.mean().item()
        scale_factor = wv.scale_factor
        add_offset = wv.add_offset

        # test with scale and offset
        wv = xa[var]
        assert(not hasattr(wv, "scale_factor"))
        assert(not hasattr(wv, "add_offset"))
        arr = wv[fixtures[0]["aoi"]]
        
        self.assertAlmostEqual(arr.mean().item(), mean * scale_factor + add_offset, places=5)

    def test_fillvalue(self):
        # use patched zarr metadata file as workaround to incorrect DMR++ files showing Int8 datasets as Int16
        fix = fixtures[2]
        bname = f"{os.path.splitext(os.path.basename(fix['url']))[0]}.zarr.json"
        with open(os.path.join(testpath, 'fixtures', bname)) as f:
            meta = json.loads(f.read())

        store = ConsolidatedChunkStore(meta, fix["url"])

        xa = xarray.open_zarr(store, mask_and_scale=True)

        var = 'sea_surface_temperature'

        # do not apply mask - check that fill value exists and calculate mean excluding them
        xa_nofill = xarray.open_zarr(store, mask_and_scale=False)
        arr = xa_nofill[var][fix["aoi"]].values
        locs = np.where(arr == xa_nofill['sea_surface_temperature']._FillValue)
        assert(len(locs[0]) > 0)
        mean = arr[arr != xa_nofill['sea_surface_temperature']._FillValue].mean()
        mean = mean * xa_nofill['sea_surface_temperature'].scale_factor + xa_nofill['sea_surface_temperature'].add_offset

        # apply mask and use numpy nanmean function to calculate mean
        arr2 = xa[var][fix["aoi"]].values
        mean2 = np.nanmean(arr2)

        self.assertAlmostEqual(mean, mean2, places=4)

        