from contextlib import contextmanager
import time

import requests
from cachecontrol import CacheController, CacheControlAdapter
from requests_futures.sessions import FuturesSession


class ElapsedFuturesSession(FuturesSession):

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


def _build_session():
    """Build HTTP session using Futures for caching and doing async calls

    Returns:
        session: A requests session
    """
    session = ElapsedFuturesSession()
    cache_adapter = CacheControlAdapter()
    cache_adapter.controller = CacheController(cache=cache_adapter.cache, status_codes=(200, 203, 300, 301, 303, 307))
    session.mount('http://', cache_adapter)
    session.mount('https://', cache_adapter)
    return session, cache_adapter

_profiles = {}

@contextmanager
def profiled(name):
    """Time executions and store in _profiles array

    Args:
        name (str): Name to use as key in _profiles
    """
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        prev_count, prev_duration = _profiles.get(name, (0, 0.0))
        _profiles[name] = (prev_count + 1, prev_duration + duration)

def print_profiles():
    """Print out profile information
    """
    strs = [ (name, str(count), '%.3fs' % (duration,)) for name, (count, duration) in _profiles.items()]
    strs.insert(0, ('Name', '#', 'Time'))
    str_widths = map(lambda x: map(len, x), strs)
    w0, w1, w2 = map(max, list(map(list, zip(*str_widths))))

    row_format = '| %s | %s | %s |'
    header_row, *body_rows = strs
    header_str = row_format % (header_row[0].center(w0), header_row[1].center(w1), header_row[2].center(w2))
    separator = '-' * len(header_str)
    print(separator)
    print(header_str)
    print(separator)
    for row in body_rows:
        print(row_format % (row[0].ljust(w0), row[1].rjust(w1), row[2].rjust(w2)))
    print(separator)

session, cache_adapter = _build_session()