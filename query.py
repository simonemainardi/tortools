import pycurl
import StringIO

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass

class Query(object):
    """
    Queries remote urls with pycurl
    """
    def __init__(self):
        _query = pycurl.Curl()
        _query.setopt(pycurl.FOLLOWLOCATION, 1)
        _query.setopt(pycurl.MAXREDIRS, 5)
        _query.setopt(pycurl.CONNECTTIMEOUT, 60)
        _query.setopt(pycurl.TIMEOUT, 300)
        _query.setopt(pycurl.NOSIGNAL, 1)
        self._query = _query

class QuerySocks(Query):
    """
    Queries remote url with pycurl over a socks proxy
    """
    def __init__(self, socks_port):
        Query.__init__(self)
        self._query.setopt(pycurl.PROXY, 'localhost')
        self._query.setopt(pycurl.PROXYPORT, socks_port)
        self._query.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS4)

    def fetch(self, url):
        """
        Uses pycurl to fetch a site using the proxy.
        """
        output = StringIO.StringIO()
        self._query.setopt(pycurl.WRITEFUNCTION, output.write)
        self._query.setopt(pycurl.URL, url)
    
        try:
            self._query.perform()
            return output.getvalue()
        except pycurl.error as exc:
            return "Unable to reach %s (%s)" % (url, exc)
