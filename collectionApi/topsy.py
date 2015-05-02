#!/usr/bin/env python

__processor__ = 'resource'
import requests
import urllib
import urlparse
import json
import logging
from datetime import timedelta
import datetime
import time
from pytz import utc

log = logging.getLogger(__processor__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())


class Resource(object):
    """Base class for calling Otter REST API
    >>> import otter
    >>> kw = otter.loadrc() # load beta key
    >>> r = Resource('search', **kw)
    >>> r(q='san francisco', window='h')
    >>> for page in r:
    ...   for item in page.response.list:
    ...     print item.title
    ...     print item.url
    """

    def __init__(self, base_uri=None, resource='trackbacks', format='json', 
            type='api.topsy', **kwargs):
        """resource = otter REST resource name (ie. search),
        format = (only json is supported right now),
        kw = keyword args to pass to the api"""
        if base_uri:
            self.scheme, netloc, path, _, _, _ = urlparse.parse(base_uri)
            self.base_uri = netloc if netloc else path
            if not self.scheme:
                self.scheme = 'http'
            if 'otter' in base_uri:
                self.api_type = 'otter'
            else:
                self.api_type = 'api.topsy'
        else:
            self.scheme = 'http'
            if type == 'api.topsy':
                self.base_uri = 'api.topsy.com/v2'
                self.type = type
            elif type == 'otter':
                self.base_uri = 'otter.topsy.com'
                self.type = type
            else:
                raise NotImplementedError

        if not 'perpage' in kwargs:
            kwargs['perpage'] = 20000
            self.pagesize = kwargs['perpage']

        kwargs['limit'] = 20000
        self.resource = resource
        self.format = format
        self.content = None  # store decoded json
        default_args = {'include_enrichment_all': 1, 'sort_by': 'date'}
        self.kwargs  = default_args
        self.kwargs.update(kwargs)
        self.pagenum = 1
        self.num_null_windows = 1
        self.total_retrieved = 0
        self.total_available = 0

    def get_params(self):
        params = {'type': self.type, 'base_uri': self.base_uri, 'resource': self.resource,
                'format': self.format}
        params.update(self.kwargs)
        return params

    def make_url(self, resource=None, response_format=None, **kwargs):
        """staticmethod to constuct an api url"""
        resource = self.resource if not resource else resource
        response_format = self.response_format if not response_format else response_format
        query = urllib.urlencode(kwargs)
        parts = (
            self.scheme,
            self.base_uri,
            '%s.%s' % (resource, response_format),
            '',  # params
            query,
            ''  # fragment
        )
        return urlparse.urlunparse(parts)

    @staticmethod
    def get(url):
        """use requests to get the url and return decoded json"""
        r = requests.get(url)
        try:
            return r.json()
        except:
            jmsgs = []
            for l in r.iter_lines():
                jmsgs.append(json.loads(l))
            return jmsgs

    def __call__(self, **kwargs):
        """call (HTTP GET) the API resource, return self"""
        self.kwargs.update(kwargs)
        self.url = self.make_url(self.resource, self.format, **self.kwargs)
        result = self.get(self.url)
        self.content = result
        return self.content

    def next_page(self):
        """fetch the next page"""
        if self.content is None:
            raise RuntimeError('must get resource before calling next')

        if self.type == 'otter':
            print self.type
            page = int(self.content['response']['page']) + 1
            last_offset = int(self.content['response']['last_offset'])
            total = int(self.content['response']['total'])
            self.pagenum = page
            current_num_tweets_retrieved = len(self.content['response']['list'])
        else:
            if 'offset' not in self.content['request']:
                last_offset = self.pagenum * self.pagesize
            else:
                last_offset = self.content['request']['offset'][0] + self.pagesize
            self.pagenum += 1
            page = self.pagenum
            total = int(self.content['response']['results']['url_info']['metrics']['citations']['total'])
            current_num_tweets_retrieved = len(self.content['response']['results']['citations'])
 
        self.total_retrieved += current_num_tweets_retrieved
        log.info('retrieved-%s, total-%s' % (current_num_tweets_retrieved, total))
        if self.total_available < total:
            self.total_available = total

        if current_num_tweets_retrieved:
            self.maxtime = self.get_timebound(bound='max')
            self.mintime = self.get_timebound(bound='min')
            print datetime.datetime.fromtimestamp(self.mintime)
            self.num_null_windows = 0
        else:
            if self.num_null_windows >= 1:
                delta = (datetime.datetime.fromtimestamp(self.maxtime) -
                        datetime.datetime.fromtimestamp(self.mintime))
                self.mintime = (datetime.datetime.fromtimestamp(self.mintime) - delta)
                self.mintime = int(time.mktime(utc.localize(self.mintime).utctimetuple()))
            print datetime.datetime.fromtimestamp(self.mintime)
            if self.kwargs['sort_by'] == 'date':
                self.kwargs['maxtime'] = self.mintime
            else:
                self.kwargs['mintime'] = self.maxtime

            self.pagenum = 1
            #last_offset = 0
            self.num_null_windows += 1

        if last_offset < self.total_available and self.num_null_windows <= 3:
            log.info('time bound- max-{}, min-{}'.format(self.maxtime, self.mintime) )
            self.kwargs['offset'] = last_offset
            self.kwargs['page'] = page
            return self(**self.kwargs)
        
        raise Exception('pagination complete, total-{}, last_offset-{}, num_shifts_made-{}'.format(total, self.total_retrieved, self.num_null_windows))

    def get_timebound(self, bound='max'):
        if self.kwargs['sort_by'] == '-date':
            bound_index = -1 if bound == 'max' else 0
        else:
            bound_index = 0 if bound == 'max' else -1

        if self.type == 'otter':
            recentTweetDate = self.content['response']['list'][bound_index]['date']
        else:
            recentTweetDate = self.content['response']['results']['citations'][bound_index]['citation_date']
    
        return recentTweetDate

    def __iter__(self):
        """iterate over all the pages"""
        if not self.content:
            self()
        def gen():
            if self.content:
                try:
                    while True:
                        yield self.content
                        self.next_page()
                except Exception, e:
                    log.exception('Unable to iter over all pages:{}'.format(e))
        return gen()

    def __getattr__(self, name):
        """quick access to response and request json objects"""
        if name == 'response':
            return self.content.response
        elif name == 'request':
            return self.content.request
        raise AttributeError


class JsonObject(object):
    """Wrapper around Json object and array"""

    def __init__(self, msg):
        self.msg = msg

    def __getattr__(self, name):
        try:
            if isinstance(self.msg, list):
                # allow querying of items in a list
                # for example: response.list.url
                r = []
                for i in self.msg:
                    a = i[unicode(name)]
                    if isinstance(a, dict) or isinstance(a, list):
                        r.append(JsonObject(a))
                    else:
                        r.append(a)
                return JsonObject(r)
            else:
                a = self.msg[unicode(name)]
                if isinstance(a, dict) or isinstance(a, list):
                    return JsonObject(a)
                else:
                    return a
        except KeyError:
            pass
        raise AttributeError("%s not found" % name)

    def __str__(self):
        return str(self.msg)

    def __unicode__(self):
        return unicode(self.msg)

    def __iter__(self):
        def gen():
            for i in self.msg:
                yield JsonObject(i)
        return gen()

    def dumps(self, encoding='utf-8'):
        jstr = json.dumps(self.msg, ensure_ascii=False)
        if encoding:
            jstr = jstr.encode('utf-8')

        return jstr

    def dump(self, outfile, encoding='utf-8'):
        jstr = json.dumps(self.msg, ensure_ascii=False)
        if encoding:
            jstr = jstr.encode('utf-8')

        outfile.write(jstr)
        return

if __name__ == '__main__':
    import doctest
    doctest.testmod()
