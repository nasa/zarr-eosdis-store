import logging
import re
import time

from cachecontrol import CacheController, CacheControlAdapter
import requests
from requests_futures.sessions import FuturesSession
import xml.etree.ElementTree as ElementTree

from .dmrpp import to_zarr
from .version import __version__
from zarr.storage import ConsolidatedMetadataStore

logger = logging.getLogger(__name__)


class ElapsedFuturesSession(FuturesSession):
    """Track start time and elapsed time for all requests in this session

    Args:
        FuturesSession (FuturesSession): Parent class
    """

    def request(self, method, url, hooks={}, *args, **kwargs):
        start = time.time()

        def timing(r, *args, **kwargs):
            r.start = start
            r.elapsed = time.time() - start

        try:
            if isinstance(hooks['response'], (list, tuple)):
                # needs to be first so we don't time other hooks execution
                hooks['response'].insert(0, timing)
            else:
                hooks['response'] = [timing, hooks['response']]
        except KeyError:
            hooks['response'] = timing

        return super(ElapsedFuturesSession, self) \
            .request(method, url, hooks=hooks, *args, **kwargs)


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

        # create futures session
        self.session = ElapsedFuturesSession()
        cache_adapter = CacheControlAdapter()
        cache_adapter.controller = CacheController(
            cache=cache_adapter.cache,
            status_codes=(200, 203, 300, 301, 303, 307)
        )
        self.session.mount('http://', cache_adapter)
        self.session.mount('https://', cache_adapter)

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
            yield future.result()

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
        request = self.session.get(self.url, headers={
            'Range': 'bytes=' + range_str,
            'User-Agent': f'zarr-eosdis-store/{__version__}'
        })
        if self.first_fetch:
            self.first_fetch = False
            request.result()
        return request


class ConsolidatedChunkStore(ConsolidatedMetadataStore):
    """Zarr store for performing range reads on remote HTTP resources in a way that parallelizes
    and combines reads.

    Args:
        ConsolidatedMetadataStore (ConsolidatedMetadataStore): Parent class using single source of metadata
    """
    def __init__(self, meta_store, data_url):
        """Instantiate ConsolidatedChunkStore

        Args:
            meta_store (dict): A Python object with the structure of a consolidated Zarr metadata store
            data_url (str): URL to data file
        """
        self.meta_store = meta_store
        self.chunk_source = HttpByteRangeReader(data_url)

    def __getitem__(self, key):
        """Get an item from the store

        Args:
            key (str): Key of the item to fetch from the store as defined by Zarr

        Returns:
            The data or metadata value of the item
        """
        return self.getitems((key, ))[key]

    def getitems(self, keys, **kwargs):
        """Get values for the provided list of keys from the Zarr store

        Args:
            keys (Array): Array of string keys to fetch from the store

        Returns:
            An iterator returning tuples of the input keys to their data or metadata values
        """
        return dict(self._getitems_generator(keys, **kwargs))

    def _getitems_generator(self, keys, **kwargs):
        """Generate results for getitems
        """
        ranges = []
        for key in keys:
            if re.search(r'/\d+(\.\d+)*$', key):
                # The key corresponds to a chunk within the file, look up its offset and size
                path, name = key.rsplit('/', 1)
                chunk_loc = self.meta_store[path + '/.zchunkstore'][name]
                ranges.append((key, chunk_loc['offset'], chunk_loc['size']))
            else:
                # Metadata key, return its value
                yield (key, super().__getitem__(key))

        # Get all the byte ranges requested
        for k, v in self._getranges(ranges).items():
            yield (k, v)

    def _getranges(self, ranges):
        '''Given a set of byte ranges [(key, offset, size), ...], fetches and returns a mapping of keys to bytes

        Args:
            ranges (Array): Array of desired byte ranges of the form [(key, offset, size), ...]
        Returns:
            dict-like [(key, bytes), (key, bytes), ...]
        '''
        reader = self.chunk_source
        ranges = sorted(ranges, key=lambda r: r[1])
        merged_ranges = self._merge_ranges(ranges)
        range_data_offsets = [r[-1] for r in merged_ranges]
        logger.debug(f"Merged {len(ranges)} requests into {len(range_data_offsets)}")

        range_data = reader.read_ranges([(offset, size) for offset, size, _ in merged_ranges])
        self.responses = list(range_data)
        range_data = [r.content for r in self.responses]
        result = self._split_ranges(zip(range_data_offsets, range_data))
        return result

    def _split_ranges(self, merged_ranges):
        '''Given tuples of range groups as returned by _merge_ranges and corresponding bytes,
        returns a map of keys to corresponding bytes.

        Args:
            merged_ranges (Array): Array of (group, bytes) where group is as returned by _merge_ranges
        Returns:
            dict-like [(key, bytes), (key, bytes), ...]
        '''
        result = {}
        for ranges, data in merged_ranges:
            for key, offset, size in ranges:
                result[key] = data[offset:(offset+size)]
        return result

    def _merge_ranges(self, ranges, max_gap=10000):
        '''Group an array of byte ranges that need to be read such that any that are within `max_gap`
        of each other are in the same group.

        Args:
            ranges (Array): An array of tuples of (key, offset, size)
        Returns:
            An array of groups of near-adjacent ranges
                [
                    [
                        offset, # The byte offset of the group from the start of the file
                        size,   # The number of bytes that need to be read
                        [
                            (   # Range within group
                                key,        # The key from the input tuple
                                sub-offset, # The byte offset of the range from the start of the group
                                size        # The number of bytes for the range
                            ),
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
                logger.debug("Starting new range due to gap of %d bytes" % (offset - prev_offset,))
                result.append((group_offset, prev_offset - group_offset, group))
                group_offset = offset
                group = []
            group.append((key, offset - group_offset, size))
            prev_offset = offset + size
        result.append((group_offset, prev_offset - group_offset, group))
        return result


class EosdisStore(ConsolidatedChunkStore):
    """Store representing a HDF5/NetCDF file accessed over HTTP with zarr metadata derived from a DMR++ file

    Args:
        ConsolidatedChunkStore (ConsolidatedChunkStore): Parent class is a store for doing byte range reads
    """
    def __init__(self, data_url, dmr_url=None):
        """Construct the store

        Args:
            data_url (String): The URL of the remote data file which should be accessed through Zarr
            dmr_url (String): Optional URL to a DMR++ file describing metadata and byte offsets of the
            given file.  If not provided, the URL is assumed to be the original file with a .dmrpp suffix
        """
        if dmr_url is None:
            dmr_url = data_url + '.dmrpp'
        dmrpp = requests.get(dmr_url).text
        tree = ElementTree.fromstring(dmrpp)
        meta_store = to_zarr(tree)
        super(EosdisStore, self).__init__(meta_store, data_url)
