from utils import status
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess
from scrapy.signals import item_scraped

import os
import scrapy
import uuid
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from urlparse import urlparse
import json
import re
import logging
import httplib
import urllib
import urllib2

status_writer = status.Writer()
progress_data = {"counter" : 0}


class SiteCrawlerSpider(CrawlSpider):
    name = 'crawler-spider'
    # download_delay = 0.25

    rules = (
        Rule(LinkExtractor(), callback="parse_item", follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(SiteCrawlerSpider, self).__init__(**kwargs)
        self.job = kwargs.get('job')
        self.start_urls = kwargs.get('start_urls')
        self.allowed_domains = kwargs.get('allowed_domains')

    def check_url(self, url):
        try:
            req = urllib2.Request(url=url)
            resp = urllib2.urlopen(req, timeout=5)
            return resp.code < 400
        except Exception as e:
            logging.exception(e)
        return False

    def make_id(self, url):
        return str(uuid.uuid3(uuid.NAMESPACE_URL, url))

    def parse_item(self, response):
        path = urlparse(response.url).path[1:]
        parts = path.split('/')[:-1]
        domain = urlparse(response.url).hostname
        taxo_parts = ['/' + '/'.join(parts[:index+1]) for index in range(len(parts))]
        path = response.url
        filename, file_extension = os.path.splitext(response.url)

        desc = response.xpath("//meta[@name='description']/@content").extract_first()

        if not file_extension:
            # try seeing if adding an index.html to the end will make it work 
            p = response.url.replace('?%s' % urlparse(response.url).query, '')
            if not p.endswith('/'):
                p += '/'
            if self.check_url(p + 'index.html') and 'jobs.bp.com' not in path:
                if urlparse(response.url).query:
                    path = p + 'index.html?' + urlparse(response.url).query
                else: 
                    path = p + 'index.html'
                file_extension = '.html'

        if not file_extension:
            entry = {
                'action': 'ADD',
                'location': self.job.location_id,
                'entry': {
                    'fields': {
                        'id': self.make_id(response.url),
                        '__to_extract': False,
                        'description': desc,
                        'name': response.url,
                        'text': response.text,
                        'fss_site_map': taxo_parts,
                        'fs_domain': domain,
                        'hasThumb': True,
                        'image_url': 'https://www.bp.com/content/bp/en/global/corporate/jcr:content/herocarousel/image3.img.960.high.jpg/1510924672158.jpg'
                    }
                }
            }
        else:
            entry = {
                'action': 'UPDATE',
                'location': self.job.location_id,
                'path': path,
                'entry': {
                    'fields': {
                        'id': self.make_id(response.url),
                        '__to_extract': True,
                        'name': response.url,
                         # default the image url in case the extractor has a problem making thumbs
                        'image_url': 'https://www.bp.com/content/bp/en/global/corporate/jcr:content/herocarousel/image3.img.960.high.jpg/1510924672158.jpg',
                        'description': desc,
                        'fss_site_map': taxo_parts,
                        'fs_domain': domain,
                        '__keep_fields': json.dumps({
                            'description': desc,
                            'fss_site_map': taxo_parts,
                            'fs_domain': domain
                        })
                    }
                }
            }
        self.job.send_entry(entry)


def run_job(job):
    """Worker function to run a scrapy crawler and index the results"""
    job.connect_to_zmq()
    config = job.scrapy_config
    process = CrawlerProcess()

    process.crawl(SiteCrawlerSpider, start_urls=config['start_urls'], allowed_domains=config['allowed_domains'], job=job)
    process.start()

import base_job
run_job( base_job.Job(r'D:\foo\job.json'))