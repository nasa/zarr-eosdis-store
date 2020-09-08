import json
import os
import unittest
import xml.etree.ElementTree as ElementTree
import eosdis_store.dmrpp as dmr

from eosdis_store.common import session, profiled, print_profiles


testpath = os.path.dirname(__file__)


class Test(unittest.TestCase):

    test_files = [
        'https://harmony.uat.earthdata.nasa.gov/service-results/harmony-uat-staging/public/demo/zarr-store/f16_ssmis_20051022v7.nc.dmrpp',
        'https://harmony.uat.earthdata.nasa.gov/service-results/harmony-uat-staging/public/demo/zarr-store/3B-HHR.MS.MRG.3IMERG.20051022-S000000-E002959.0000.V06B.HDF5.dmrpp',
        #'https://harmony.uat.earthdata.nasa.gov/service-results/harmony-uat-staging/public/demo/zarr-store/3B-HHR.MS.MRG.3IMERG.20051022-S233000-E235959.1410.V06B.HDF5.dmrpp',
    ]

    @classmethod
    def get_test_xml(cls, idx=0):
        with profiled('Get DMR++'):
            dmrpp = session.get(cls.test_files[idx]).result().text
            tree = ElementTree.fromstring(dmrpp)
        return tree

    @classmethod
    def tearDownClass(cls):
        print()
        print_profiles()

    def test_find_child(self):
        tree = self.get_test_xml()
        node = dmr.find_child(tree, 'HDF5_GLOBAL')
        assert(node.attrib['name'] == 'HDF5_GLOBAL')

    def test_get_attribute_values(self):
        xml = '<Attribute name="axis" type="String"><Value>T</Value></Attribute>'
        node = ElementTree.fromstring(xml)
        vals = dmr.get_attribute_values(node)
        assert(vals == 'T')
        # TODO - test lists

    def test_get_attributes(self):
        tree = self.get_test_xml()
        node = dmr.find_child(tree, 'HDF5_GLOBAL')
        arr = dmr.get_attributes(node)
        assert(arr['chunksize'] == '90')
        assert(arr['Conventions'] == 'CF-1.6')
        assert(arr['numberofpasses'] == '2')

    def test_get_dimensions(self):
        tree = self.get_test_xml()
        dims = dmr.get_dimensions(tree)
        assert(len(dims) == 3)
        assert('/time' in dims.keys())
        assert('/latitude' in dims.keys())
        assert('/longitude' in dims.keys())
        assert(dims['/latitude']['size'] == 720)

    def test_chunks_to_zarr_single(self):
        tree = self.get_test_xml()
        node = tree.find(".//d:Float32[@name='latitude']/dpp:chunks", dmr.NS)
        chunks = dmr.chunks_to_zarr(node)
        assert('zarray' in chunks)
        assert('zchunkstore' in chunks)
        assert(chunks['zchunkstore']['0']['offset'] == 106784)
        assert(chunks['zchunkstore']['0']['size'] == 2880)

    def test_chunks_to_zarr_multi(self):
        tree = self.get_test_xml()
        node = tree.find(".//d:Int16[@name='sst_dtime']/dpp:chunks", dmr.NS)
        chunks = dmr.chunks_to_zarr(node)
        assert('zarray' in chunks)
        assert('zchunkstore' in chunks)
        assert(len(chunks['zchunkstore']) == 128)
        assert(chunks['zchunkstore']['0.7.15']['size'] == 4324)

    def test_array_to_zarr(self):
        tree = self.get_test_xml()
        dims = dmr.get_dimensions(tree)
        assert(dims['/longitude']['size'] == 1440)
        # test on wind_speed array
        node = tree.find(".//d:Int16[@name='wind_speed']", dmr.NS)
        arr = dmr.array_to_zarr(node, dims)
        assert('wind_speed/.zarray' in arr)
        assert('wind_speed/.zattrs' in arr)
        assert('wind_speed/.zchunkstore' in arr)
        assert(arr['wind_speed/.zattrs']['_ARRAY_DIMENSIONS'] == ['time', 'latitude', 'longitude'])
        assert(arr['wind_speed/.zchunkstore']['0.6.11']['size'] == 888)
        # test on latitude array
        node = tree.find(".//d:Float32[@name='latitude']", dmr.NS)
        arr = dmr.array_to_zarr(node, dims)
        assert('latitude/.zarray' in arr)
        assert('latitude/.zattrs' in arr)
        assert('latitude/.zchunkstore' in arr)
        assert(arr['latitude/.zattrs']['_ARRAY_DIMENSIONS'] == ['latitude'])
        assert(arr['latitude/.zchunkstore']['0']['size'] == 2880)

    def test_to_zarr(self):
        tree = self.get_test_xml()
        zarr = dmr.to_zarr(tree)
        with open(os.path.join(testpath, 'fixtures', 'f16_ssmis_20051022v7.zarr.json')) as f:
            fixture = json.loads(f.read())
        json1 = json.dumps(fixture, sort_keys=True)
        json2 = json.dumps(zarr, sort_keys=True)
        #with open('tests/zarr-metadata-test.json', 'w') as f:
        #    f.write(json.dumps(zarr))
        assert(json1 == json2)

    def test_to_zarr_more_examples(self):
        for i in range(1, len(self.test_files)):
            tree = self.get_test_xml(i)
            zarr = dmr.to_zarr(tree)
            bname = os.path.splitext(os.path.basename(self.test_files[i].replace('.dmrpp', '')))[0]
            with open(os.path.join(testpath, 'fixtures', f"{bname}.zarr.json")) as f:
                fixture = json.loads(f.read())
            json1 = json.dumps(fixture, sort_keys=True)
            json2 = json.dumps(zarr, sort_keys=True)
            with open(os.path.join(testpath, f"{bname}.zarr.json"), 'w') as f:
                f.write(json.dumps(zarr))
            assert(json1 == json2)