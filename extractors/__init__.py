#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from joblib import Parallel, delayed
import requests
from goose import Goose
from langdetect import detect as detect_lang
from goose.text import StopWordsArabic
from goose.text import StopWordsChinese
from goose.text import StopWordsKorean
import sys
from bs4 import UnicodeDammit, BeautifulSoup
from urlparse import urlparse


class BaseExtractor(object):
    """
    Template for Extractors
    """
    def __init__(self):
        pass

    def extract(self):
        pass


class HTMLExtractor(BaseExtractor):
    """
    Extracts
    """
    def extract(self, url):
        if not urlparse(url).scheme:
            url = "http://" + url
        resp = requests.get(url, timeout=50)
        if resp.status_code == 200:
            resp = self.contentFix(resp)
            msg = {
                'encoding': resp.encoding,
                'content': resp.text,
                'headers': dict(resp.headers),
                'link': resp.url,
                'metadata': self.get_metadata(resp.text)
            }

            return msg

        return {'error': resp.status_code}

    def contentFix(self, resp):
        try:
            encodings = requests.utils.get_encodings_from_content(resp.content)
            if encodings and encodings[0] != resp.encoding:
                resp.encoding = encodings[0]

            return resp
        except Exception, e:
            raise Exception('Error Fixing Content (%s): %s' % (str(e), resp.url))

    def get_metadata(self, raw_html):
        soup = BeautifulSoup(raw_html)
        metadata = {}
        for meta in soup.find_all('meta'):
            if 'name' in meta.attrs:
                metadata[meta['name']] = meta.attrs.get('content', '')

        return metadata


class GooseExtractor(HTMLExtractor):
    def __init__(self):
        self.extractor = Goose()

    def extract(self, url=None, raw_html=None, default_lang='es'):
        """
        Code by Mike Ogren CACI Inc.,
        """
        if url:
            msg = super(GooseExtractor, self).extract(url)
            if not 'error' in msg:
                content_lang = self.guess_language(msg["content"])
                if not content_lang:
                    content_lang = default_lang
                msg['url_language'] = content_lang
                raw_html = msg["content"]
            else:
                return msg
            #resp = requests.get(url)
            #if resp.status_code == 200:
            #    resp = self.contentFix(resp)
            #    msg = {
            #        'encoding': resp.encoding,
            #        'headers': dict(resp.headers),
            #        'link': resp.url
            #        'metadata': {},
            #    }
            #    content_lang = self.guess_language(resp.text)
            #    msg['metadata'] =
            #    if not content_lang:
            #        content_lang = default_lang
            #    msg['url_language'] = content_lang
            #    raw_html = resp.text
            #else:
            #    return {'error': resp.status_code}
        elif raw_html:
            msg = {
                'encoding': UnicodeDammit(raw_html).original_encoding,
                'headers': '',
                'link': ''
            }
            content_lang = self.guess_language(raw_html)
            if not content_lang:
                content_lang = default_lang
            msg['url_language'] = content_lang
            raw_html = raw_html.decode(msg['encoding'])
        else:
            raise NotImplementedError

        goose_config = {
            'use_meta_language': False,
            'target_language': content_lang
        }

        #Special Goose requirements for Arabic, Chinese and some more special languages
        if content_lang == "ar":
            goose_config['stop_words_class'] = StopWordsArabic
        elif content_lang == "zh":
            goose_config['stop_words_class'] = StopWordsChinese
        elif content_lang == 'ko':
            goose_config['stop_words_class'] = StopWordsKorean

        goose_article = Goose(goose_config).extract(raw_html=raw_html)
        msg["content"] = goose_article.cleaned_text if goose_article else None
        return msg

    def guess_language(self, resp):
        try:
            article = self.extractor.extract(raw_html=resp)
            lang = article.meta_lang

            if lang is None or lang == "en":
                lang = detect_lang(resp)

            if lang == "sp":
                lang = "es"

            return lang
        except KeyError, e:
            sys.stderr.write("Error detecting language %s" % str(e))
            return None


class URLMiner(object):
    def __init__(self, n_jobs=-1, extract_content=None, **kwargs):
        """
        n_jobs: default is 1. if -1 is given then the number of processors is used
        extract_content : dump raw html by default, if True use goose
        """
        if extract_content:
            self.extractor = GooseExtractor
        else:
            self.extractor = HTMLExtractor

        self.n_jobs = n_jobs

    def extract(self, urllist, urltag=None, urlContentKey='urlcontent'):
        unwrap = False
        if isinstance(urllist, basestring):
            urllist = [urllist]
            unwrap = True

        url_content = Parallel(n_jobs=self.n_jobs, backend='threading')(delayed(urlextract)(((url[urltag] if urltag else url)), self.extractor) for url in urllist)
        if urltag:
            for index, l in enumerate(url_content):
                urllist[index][urlContentKey] = l
            url_content = urllist

        if unwrap:
            url_content = url_content[0]

        return url_content


def urlextract(url, extractor):
    return extractor().extract(url=url)
