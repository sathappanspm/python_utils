#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"


import urllib2
import requests
import feedparser
from bs4 import BeautifulSoup as bs4
from genericUtils.exceptionUtils import TimeOutError, timeout

@timeout(5)
def _isFeed(url):
    """
    Return if given url is an rss/atom feed
    params:
        url - string
    """
    f = feedparser.parse(url)
    if len(f.entries) > 0:
        return True
    else:
        return False


def get_feeds(site, recursive_check=True):
    """
    Find rss feeds for the given site
    params:
        site - url of the site
        recursize_check - recursive check flag. Sometimes an rss link contains list of sub-rss links.
                     if this flag is set, then the rss link is resolved into those sub-rss links else
                     only the top-level is returned
    """
    result = []
    try:
        raw = requests.get(site, timeout=10).text
        possible_feeds = []
        html = bs4(raw)
        feed_urls = html.findAll("link", rel="alternate")
        for f in feed_urls:
            t = f.get("type", None)
            if t:
                if "rss" in t or "xml" in t:
                    href = f.get("href", None)
                    if href:
                        possible_feeds.append(href)

        parsed_url = urllib2.urlparse.urlparse(site)
        base = parsed_url.scheme + "://" + parsed_url.hostname
        atags = html.findAll("a")
        def get_hreflink(a_item):
            if 'href' in a.attrs:
                return a.get("href", None)

            for attr in a_item.attrs:
                val = a.get(attr)
                if isinstance(val, basestring) and val.startswith("http"):
                    return a.get(attr)

        for a in atags:
            href = get_hreflink(a)
            if href:
                href = urllib2.urlparse.urljoin(base, href)
                if "xml" in href or "rss" in href or "feed" in href:
                    possible_feeds.append(href)

        possible_feeds = set(possible_feeds)
        if recursive_check and len(possible_feeds) > 5:
            recursive_check = False

        for url in set(possible_feeds):
            try:
                if _isFeed(url):
                    if url not in result:
                        result.append(url)
                else:
                    if recursive_check:
                        result.extend(get_feeds(url, False))
            except Exception, e:
                raise e
        return result
    except UnicodeDecodeError:
        return result
