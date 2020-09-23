import json
import os
import unittest

import requests
import xml.etree.ElementTree as ElementTree
import zarr

from eosdis_store import EosdisStore

testpath = os.path.dirname(__file__)


class Test(unittest.TestCase):

    data_url = 'https://harmony.uat.earthdata.nasa.gov/service-results/harmony-uat-staging/public/demo/zarr-store/f16_ssmis_20051022v7.nc'
    dmr_url = data_url + '.dmrpp'

    aoi = (0, slice(400, 549, None), slice(1040, 1261, None))

    @classmethod
    def get_test_xml(cls):
        dmrpp = requests.get(cls.dmr_url).text
        tree = ElementTree.fromstring(dmrpp)
        return tree

    @classmethod
    def open_eosdis_store(cls):
        return zarr.open(EosdisStore(cls.data_url))

    def test_eosdis_store_metadata(self):
        store = EosdisStore(self.data_url, self.dmr_url)
        keys = list(store.keys())
        assert(len(keys) == 26)
        # test default dmr_url
        store = EosdisStore(self.data_url)
        keys = list(store.keys())
        assert(len(keys) == 26)

    def test_eosdis_store_open(self):
        store = self.open_eosdis_store()
        arrays = list(store.arrays())
        assert(len(arrays) == 8)
        assert(arrays[0][0] == 'atmosphere_cloud_liquid_water_content')
        arr = arrays[0][1]
        assert(type(arr) == zarr.core.Array)
        assert(arr.name == '/atmosphere_cloud_liquid_water_content')       
        assert(arr.shape == (2, 720, 1440))

    def test_eosdis_store_read(self):
        store = self.open_eosdis_store()
        arr = store['wind_speed'][self.aoi]
        assert(arr.shape == (149, 221))
        assert(arr[0][0] == 19)
        assert(arr.mean() == 169.29050381123022)

    def test_eosdis_store_getranges_combined(self):
        store = EosdisStore(self.data_url)
        ranges = [
            ('wind_speed/0.4.11', 768280, 6830),
            ('wind_speed/0.4.12', 775112, 5759)
        ]
        result = store._getranges(ranges)
        assert(len(result) == 2)
        assert(len(store.responses) == 1)

    def test_eosdis_store_getranges_split(self):
        store = EosdisStore(self.data_url)
        ranges = [
            ('wind_speed/0.4.11', 768280, 6830),
            ('wind_speed/0.4.12', 785112, 5759)
        ]
        result = store._getranges(ranges)
        assert(len(result) == 2)
        assert(len(store.responses) == 2)

    def test_eosdis_store_parallel_reads(self):
        store = self.open_eosdis_store()
        arr = store['wind_speed'][self.aoi]
        responses = store.store.responses
        end_time = responses[0].start + responses[1].elapsed
        for r in responses[1:]:
            assert(r.start < end_time)
