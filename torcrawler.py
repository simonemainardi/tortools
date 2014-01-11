#!/usr/bin/env python
from torhandler import TorHandler
from multiprocessing import Process, JoinableQueue as Queue

import StringIO
import pycurl
import logging
import os

logging.basicConfig(level=logging.DEBUG,format='[%(threadName)-10s] %(message)s')

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass

class TorCrawler(TorHandler):
    """
    Parallel scraper that uses tor to anonimyze traffic.
    """
    def __init__(self, num_tor_instances=1, curlmulti_handles_per_process = 3,
                 base_socks_port = 8000, results_directory="results/"):
        """Initialize the crawler.

        In the implementation we use the
        mechanisms available in the pycurl CurlMulti() method.
        floor(num_tor_instances / curlmulti_handles_per_process) crawler processes
        are launched after tor instances are up and running.
        Each crawler process uses a number of curl handles equal to
        curlmulti_handles_per_process in order to download data in parallel.

        Parameters
        ----------
        num_tor_instances : The number of tor instances to be launched in
                            parallel. Defaults to 1.
        curlmulti_handles_per_process : The number of curl handles that each
                            process use to crawl data in parallel. Defaults to 3.
        base_socks_port   : The first socks port that tor processes use.
                            The total number of socks ports equals num_tor_instances.
                            Ports are consecutive, i.e., for 3 tor instances,
                            ports 8000, 8001 and 8002 are used.
        results_directory : Crawler results are placed here.


        See also
        --------
        https://github.com/pycurl/pycurl/blob/master/examples/retriever-multi.py
        http://stackoverflow.com/questions/5809033/multi-request-pycurl-running-forever-infinite-loop
        http://stackoverflow.com/questions/1959240/get-many-pages-with-pycurl
        """
        if not isinstance(curlmulti_handles_per_process, int):
            raise ValueError()
        assert num_tor_instances >= curlmulti_handles_per_process
        if not isinstance(results_directory, basestring):
            raise ValueError()
        # make sure that each tor instance that is going to be launched
        # have exactly curlmulti_handles_per_process. To do so, we may
        # decrease the num_tor_instances passed as function argument
        if num_tor_instances % curlmulti_handles_per_process:
            num_tor_instances -= (num_tor_instances % curlmulti_handles_per_process)
        # now num_tor_instances is divisible by curlmulti_handles_per_process
        num_crawler_instances = num_tor_instances / curlmulti_handles_per_process
        logging.debug("I am going to launch %i tor processes and %i crawler processes" %
                      (num_tor_instances, num_crawler_instances))
        logging.debug("each crawler process will run %i curlmulti handles" %
                      (curlmulti_handles_per_process))

        super(TorCrawler,self).__init__(num_tor_instances, base_socks_port)
        self.results_directory = results_directory
        self.num_urls = 0
        self.curlmulti_handles_per_process = curlmulti_handles_per_process

    def load_urls(self, **kwargs):
        urls = ["https://www.atagar.com/echo.php"] * 100
        urls_queue = Queue()
        for url in urls:
            filename = os.path.join(self.results_directory, "doc_%03d.txt" % (urls_queue.qsize() + 1))
            urls_queue.put((url, filename))
        self.urls_queue = urls_queue
        self.num_urls = int(urls_queue.qsize())

    def _process_launcher(self, process_index, queue):
        base_socks_port = self.base_socks_port + (self.curlmulti_handles_per_process * process_index)
        base_control_port = self.base_control_port + (self.curlmulti_handles_per_process * process_index)
        # do the crawl
        # -- initialize pycurl objects
        # see
        # http://stackoverflow.com/questions/5809033/multi-request-pycurl-running-forever-infinite-loop
        # http://stackoverflow.com/questions/1959240/get-many-pages-with-pycurl
        m = pycurl.CurlMulti()
        m.handles = []
        for inst in range(self.curlmulti_handles_per_process):
            c = pycurl.Curl()
            c.socks_port = base_socks_port + inst
            c.control_port = base_control_port + inst
            c.fp = None
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.CONNECTTIMEOUT, 60)
            c.setopt(pycurl.TIMEOUT, 300)
            c.setopt(pycurl.NOSIGNAL, 1)
            c.setopt(pycurl.PROXY, 'localhost')
            c.setopt(pycurl.PROXYPORT, c.socks_port)
            c.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS4)
            m.handles.append(c)

        # Main loop
        freelist = m.handles[:]
        num_processed = 0
        while 1: # process is launched in daemon mode so the father will exit
            # If there is an url to process and a free curl object, add to multi stack
            while freelist and not queue.empty():
                url, filename = queue.get()
                c = freelist.pop()
                c.fp = open(filename, "a+b")
                c.setopt(pycurl.URL, url)
                c.setopt(pycurl.WRITEDATA, c.fp)
                m.add_handle(c)
                # store some info
                c.filename = filename
                c.url = url
            # Run the internal curl state machine for the multi stack
            while 1:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            # Check for curl objects which have terminated, and add them to the freelist
            while 1:
                num_q, ok_list, err_list = m.info_read()
                for c in ok_list:
                    c.fp.close()
                    c.fp = None
                    m.remove_handle(c)
                    logging.debug("Success: %s %s %s" % (c.filename, c.url, c.getinfo(pycurl.EFFECTIVE_URL)))
                    freelist.append(c)
                for c, errno, errmsg in err_list:
                    c.fp.close()
                    c.fp = None
                    m.remove_handle(c)
                    logging.debug("Failed: %s %s %i %s" % (c.filename, c.url, errno, errmsg))
                    freelist.append(c)
                num_done_this_round = len(ok_list) + len(err_list)
                num_processed = num_processed + num_done_this_round
                # notify that we are done
                for _ in range(num_done_this_round):
                    queue.task_done()
                if num_q == 0:
                    break
            ret = m.select(1.0)
            if ret == -1:
                logging.debug('select has timed out')

        # -- cleanup
        for c in m.handles:
            if c.fp is not None:
                c.fp.close()
                c.fp = None
            c.close()
        m.close()
            
    def crawl(self, **kwargs):
        """
        Start the crawling process using parameters specified in the class constructor.
        """
        if self.num_urls == 0:
            logging.debug("no urls have be loaded... nothing to do, returning ...")
            return None
        self.launch()
        # launch multiple tor instances using threads
        num_downloaders = self.num_tor_instances / self.curlmulti_handles_per_process
        logging.debug("launching %i crawler processes" % num_downloaders)
        for process_index in range(num_downloaders):
            pro = Process(target=self._process_launcher, args=[process_index,self.urls_queue])
            pro.daemon = True
            pro.start()
        self.urls_queue.join()
        logging.debug("I'm done :) ...")
        self.kill()

if __name__ == "__main__":
    th = TorCrawler(1,1)
    th.load_urls()
    th.crawl()
