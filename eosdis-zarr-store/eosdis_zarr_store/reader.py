import os
from .common import session, profiled

class HttpByteRangeReader():
    def __init__(self, url, quiet=False):
        self.url = url
        self.first_fetch = True
        self.quiet = quiet

    def read_range(self, offset, size):
        return self._async_read(offset, size).result().content

    def read_ranges(self, range_iter):
        futures = [self._async_read(offset, size) for offset, size in range_iter]
        for future in futures:
            with profiled('Subsequent fetches'):
                yield future.result().content

    def _async_read(self, offset, size):
        if not self.quiet:
            print('Reading %s [%d:%d] (%d bytes)' % (self.url, offset, offset+size, size))
        range_str = '%d-%d' % (offset, offset + size)
        request = session.get(self.url, headers={ 'Range': 'bytes=' + range_str })
        if self.first_fetch:
            self.first_fetch = False
            with profiled('First fetch'):
                request.result()
        return request

