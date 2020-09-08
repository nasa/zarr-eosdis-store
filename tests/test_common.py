import os
import unittest

from pdb import set_trace

from eosdis_store.common import profiled, _profiles

testpath = os.path.dirname(__file__)


class Test(unittest.TestCase):

    def test_profiled(self):
        with profiled('test'):
            pass
        assert('test' in _profiles)
        assert(len(_profiles['test']) == 2)
        assert(_profiles['test'][0] == 1)