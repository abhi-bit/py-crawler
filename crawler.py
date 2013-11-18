__author__ = "Abhishek <singhabhishek.bit@gmail.com>"
__version__ = "0.1"

import urllib2
from BeautifulSoup import *
from urlparse import urljoin
import logging
import gevent
from gevent import monkey
monkey.patch_all(thread=False)
from gevent.queue import Queue
from optparse import OptionParser


LOG_FILENAME = 'crawler.out'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#File handler
handler = logging.FileHandler(LOG_FILENAME)
handler.setLevel(logging.INFO)

#Log format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

#Add handlers to the logger
logger.addHandler(handler)

#TODO: - respect robots.txt
#      - url de-duplication


class Crawler:
    """Crawler class"""
    def __init__(self, url, limit):
        """
        Initializes Crawler class
        @param url: Input seed page
        type limit: number
        @param limit: total no. of urls to fetch
        """
        self.url = url
        self.url_count_limit = limit
        self.tasks = Queue()
        self.counter = 0

    def crawl(self, url):
        """
        Crawler function
        Takes input seed pages, and uses BeautifulSoup module to fetch links inside it
        """
        try:
            data = urllib2.urlopen(url)
        except:
            logger.info('ERROR: %s' % url)

        try:
            bs = BeautifulSoup(data.read())
            links=bs('a')
    
            for link in links:
                if ('href' in dict(link.attrs)):
                    url=urljoin(url,link['href'])
                if url.find("'") != -1: 
                    continue
                url=url.split('#')[0]
    
                if url[0:4] == 'http':
                    if self.counter < self.url_count_limit:
                        self.tasks.put(url)
                        self.counter += 1
                        logger.info('ADDED: %s' % url)
        except:
            pass
    
    def run(self):
        self.crawl(self.url)
        """For Async handling of each url"""
        while not self.tasks.empty():
            url = self.tasks.get()
            gevent.spawn(self.crawl, url).join()
            logger.info('FETCH: %s' % url)

if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [options] filename",
                          version="%prog 0.1")
    parser.add_option("-s", "--seed", dest="seed", default='http://python.org', help='seed page url ex: http://googl.com"')
    parser.add_option("-l", "--limit", dest="limit", default="100", type='int', help="no. of links to process")
    (options, args) = parser.parse_args()

    url = options.seed.split()[0]
    limit = options.limit

    c = Crawler(url, limit)
    c.run()
