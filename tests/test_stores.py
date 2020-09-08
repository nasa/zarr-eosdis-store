import json
import os
import unittest
import xml.etree.ElementTree as ElementTree
import zarr

from eosdis_store import EosdisStore
from eosdis_store.common import session, profiled, print_profiles

testpath = os.path.dirname(__file__)


class TestSSMIS(unittest.TestCase):

    data_url = 'https://harmony.uat.earthdata.nasa.gov/service-results/harmony-uat-staging/public/demo/zarr-store/f16_ssmis_20051022v7.nc'
    dmr_url = data_url + '.dmrpp'

    aoi = (0, slice(400, 549, None), slice(1040, 1261, None))

    @classmethod
    def get_test_xml(cls):
        with profiled('Get DMR++'):
            dmrpp = session.get(cls.dmr_url).result().text
            tree = ElementTree.fromstring(dmrpp)
        return tree

    @classmethod
    def tearDownClass(cls):
        print()
        print_profiles()

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
