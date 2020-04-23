from zarr.storage import ConsolidatedMetadataStore
import xml.etree.ElementTree as ElementTree
import re
from .dmrpp import dmrpp_to_zarr
from .reader import HttpByteRangeReader
from .common import session, profiled

def merge_ranges(ranges, max_gap=10000):
    '''
    Max gap: Set heuristically to merge nearby ranges such that making a new request costs about
    the same as getting the extra bytes between the ranges

    Input ranges = [(key, offset, size), ...]
    Output ranges = [
        [
            offset,
            size,
            [
                (key, sub-offset, size),
                (key, sub-offset, size),
                ...
            ]
        ],
        ...
    ]
    '''
    ranges = sorted(ranges, key=lambda r: r[1])
    if len(ranges) == 0:
        return []
    group_offset = ranges[0][1]
    prev_offset = ranges[0][1]
    group = []
    result = []
    for key, offset, size in ranges:
        if offset - prev_offset > max_gap + 1:
            print("Starting new range due to gap of %d bytes" % (offset - prev_offset,))
            result.append((group_offset, prev_offset - group_offset, group))
            group_offset = offset
            group = []
        group.append((key, offset - group_offset, size))
        prev_offset = offset + size
    result.append((group_offset, prev_offset - group_offset, group))
    return result

def split_ranges(merged_ranges):
    '''
    merged_ranges: array of ([[key, sub-offset, size], ...], bytes)
    result: dict-like [(name, bytes), (name, bytes), ...]
    '''
    result = {}
    for ranges, data in merged_ranges:
        for key, offset, size in ranges:
            result[key] = data[offset:(offset+size)]
    return result

class ConsolidatedChunkStore(ConsolidatedMetadataStore):
    def __init__(self, meta_store, chunk_source, quiet=False):
        self.meta_store = meta_store
        self.chunk_source = chunk_source
        self.quiet = quiet

    def __getitem__(self, key):
        return next(self.getitems((key, )))[1]

    def getitems(self, keys):
        ranges = []
        for key in keys:
            if re.search(r'/\d+(\.\d+)*$', key):
                path, name = key.rsplit('/', 1)
                chunk_loc = self.meta_store[path + '/.zchunkstore'][name]
                ranges.append((key, chunk_loc['offset'], chunk_loc['size']))
            else:
                yield (key, super().__getitem__(key))

        for k, v in self._getranges(ranges).items():
            yield (k, v)

    def _getranges(self, ranges):
        reader = self.chunk_source
        ranges = sorted(ranges, key=lambda r: r[1])
        merged_ranges = merge_ranges(ranges)
        range_data_offsets = [r[-1] for r in merged_ranges]
        if not self.quiet:
            print('Merged', len(ranges), 'requests into', len(range_data_offsets))

        range_data = reader.read_ranges([(offset, size) for offset, size, _ in merged_ranges])
        range_data = [r for r in range_data] # FIXME Avoids a seemingly-inconsequential GeneratorExit.  Validate it is inconsequential
        result = split_ranges(zip(range_data_offsets, range_data))
        return result

class EosdisZarrStore(ConsolidatedChunkStore):
    def __init__(self, data_url, quiet=False):
        dmr_url = data_url + '.dmrpp'
        reader = HttpByteRangeReader(data_url, quiet)
        with profiled('Get DMR++'):
            dmrpp = session.get(dmr_url).result().text
            tree = ElementTree.fromstring(dmrpp)
        with profiled('DMR++ Transform'):
            meta_store = dmrpp_to_zarr(tree)
        super(EosdisZarrStore, self).__init__(meta_store, reader, quiet)
