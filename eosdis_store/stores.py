import logging
import re
import xml.etree.ElementTree as ElementTree

from .common import session, profiled
from .dmrpp import to_zarr
from zarr.storage import ConsolidatedMetadataStore

logger = logging.getLogger(__name__)


class HttpByteRangeReader():
    """Perform HTTP range reads on remote files
    """
    def __init__(self, url):
        """Create HttpByteRangeRead instance for a single file

        Args:
            url (str): URL to remote file
        """
        self.url = url
        self.first_fetch = True

    def read_range(self, offset, size):
        """Read a range of bytes from remote file

        Args:
            offset (int): Offset, in number of bytes
            size (int): Number of bytes to read

        Returns:
            Bytes: Contents file file over range
        """
        return self._async_read(offset, size).result().content

    def read_ranges(self, range_iter):
        """Read multiple ranges simultaneously (async)

        Args:
            range_iter (iterator): List of ranges

        Yields:
            iterator: Iterator to content of each range
        """
        futures = [self._async_read(offset, size) for offset, size in range_iter]
        for future in futures:
            with profiled('Subsequent fetches'):
                yield future.result().content

    def _async_read(self, offset, size):
        """Asynchronous HTTP read

        Args:
            offset (int): Offset, in number of Bytes
            size (int): Number of bytes to read

        Returns:
            response: Return request response
        """
        logger.debug(f"Reading {self.url} [{offset}:{offset+size}] ({size} bytes)")
        range_str = '%d-%d' % (offset, offset + size)
        request = session.get(self.url, headers={ 'Range': 'bytes=' + range_str })
        if self.first_fetch:
            self.first_fetch = False
            with profiled('First fetch'):
                request.result()
        return request


class ConsolidatedChunkStore(ConsolidatedMetadataStore):
    """Zarr store for performing range reads on remote HTTP resources

    Args:
        ConsolidatedMetadataStore (ConsolidatedMetadataStore): Parent class using single source of metadata
    """
    def __init__(self, meta_store, data_url):
        """Instantiate ConsolidatedChunkStore

        Args:
            meta_store (dict): Consolidated metadata store
            data_url (str): URL to data file
        """
        self.meta_store = meta_store
        self.chunk_source = HttpByteRangeReader(data_url)

    def __getitem__(self, key):
        """Get a single range of bytes

        Args:
            key (str): Chunk key

        Returns:
            chunk: Get chunk
        """
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
        merged_ranges = self.merge_ranges(ranges)
        range_data_offsets = [r[-1] for r in merged_ranges]
        logger.debug(f"Merged {len(ranges)} requests into {len(range_data_offsets)}")

        range_data = reader.read_ranges([(offset, size) for offset, size, _ in merged_ranges])
        range_data = [r for r in range_data] # FIXME Avoids a seemingly-inconsequential GeneratorExit.  Validate it is inconsequential
        result = self.split_ranges(zip(range_data_offsets, range_data))
        return result

    @classmethod
    def split_ranges(cls, merged_ranges):
        '''
        merged_ranges: array of ([[key, sub-offset, size], ...], bytes)
        result: dict-like [(name, bytes), (name, bytes), ...]
        '''
        result = {}
        for ranges, data in merged_ranges:
            for key, offset, size in ranges:
                result[key] = data[offset:(offset+size)]
        return result

    @classmethod
    def merge_ranges(cls, ranges, max_gap=10000):
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

class EosdisStore(ConsolidatedChunkStore):
    """Store representing a HDF5/NetCDF file with zarr metadata derived from a DMR++ file

    Args:
        ConsolidatedChunkStore (ConsolidatedChunkStore): Parent class is a store for doing byte range reads
    """
    def __init__(self, data_url, dmr_url=None):
        if dmr_url is None:
            dmr_url = data_url + '.dmrpp'
        with profiled('Get DMR++'):
            dmrpp = session.get(dmr_url).result().text
            tree = ElementTree.fromstring(dmrpp)
        with profiled('DMR++ Transform'):
            meta_store = to_zarr(tree)
        super(EosdisStore, self).__init__(meta_store, data_url)
