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
import re
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger(name='requests').setLevel(level=logging.WARNING)
log = logging.getLogger()

def decode(s, encoding=None):
    """
    Decode given string into the given encoding.
    If no encoding is specified, use 'utf-8'
    params:
        s - string to be decoded
        encoding - encoding to be used. 'utf-8' if none specified
    """
    if not encoding:
        encoding = 'utf-8'

    try:
        r_val = s.decode(encoding)
    except:
        if encoding != 'utf-8':
            r_val = decode(s, 'utf-8')
        else:
            r_val = s

    return r_val


class BaseExtractor(object):
    """
    Template for Extractors
    """
    def __init__(self):
        pass

    def extract(self):
        pass


class BaseEnricher(object):
    def __init__(self, article, soup_parser):
        self.article = article
        if article.top_node:
            top_node_attrs = dict([i for i in article.top_node.items() if i[0] not in ('gravityScore', 'gravityNodes')])
            self.parser = soup_parser.find(self.top_node.tag, attrs=top_node_attrs)
        else:
            self.parser = soup_parser
    
    def extract(self):
        pass


class LinkEnricher(BaseEnricher):
    def extract(self):
        links = {}
        for l in self.parser.findAll('a', href=True):
            links[l.attrs['href']] = l.text

        return links


class TweetInfoEnricher(BaseEnricher):
    def __init__(self, article, soup_parser):
        super(TweetInfoEnricher, self).__init__(article, soup_parser)
        self.hashtagre = re.compile(r'#[^\s]*\b', re.U|re.I)
        self.userre = re.compile(r'@[^\s]*\b', re.I|re.U)

    def extract(self):
        tweet_blocks = self._extract_tweets()
        hashtags_users = self._extract_user_hashtags()
        if tweet_blocks or hashtags_users:
            res = {'tweets': tweet_blocks}
            res.update(hashtags_users)
            return res
        return {}

    def _extract_tweets(self):
        blocks = self.parser.findAll('blockquote')
        tweets = []
        for b in blocks:
            bclass = " ".join(b.attrs['class'])
            if "tweet" in bclass or 'twitter' in bclass:
                msg = {'html_block': b.__str__()}
                tweet_links = []
                if "cite" in b.attrs:
                    tweet_links += b.attrs['cite']

                if 'data-tweet-id' in b.attrs:
                    msg['tweet-id'] = b.attr['data-tweet-id']

                tweet_links += [mention.attrs['href'] for mention in b.findAll('a')
                    if '/status' in mention.attrs.get('href', '')]
                msg['tweet-link'] = list(set(tweet_links))
                tweets.append(msg)
        return tweets

    def _extract_user_hashtags(self):
        ht = self.hashtagre.findall(self.article.cleaned_text)
        user = self.userre.findall(self.article.cleaned_text)
        if ht or user:
            return {'users': user, 'hashtags': ht}
        return {}


class HTMLExtractor(BaseExtractor):
    """
    Extracts
    """
    def __init__(self):
        self.soup_parser = None

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

    def get_metadata(self, raw_html=None):
        """
        Get Meta-data from given HTML text.
        Split them based on the origin of meta (like - 'og', 'twitter', 'dc', etc.)
        params-
            raw_html: Raw HTML text

        return:
            metadata : Dict of form  {'features': {}, 'properties': {} }
        """
        self.soup_parser = BeautifulSoup(raw_html)
        metadata = {"features": {}, "properties": {}}
        for meta in self.soup_parser.find_all('meta'):
            meta_type = 'name' if 'name' in meta.attrs else ('property' if 'property' in meta.attrs else "")
            if meta_type:
                metadata['features'][meta[meta_type]] = meta.attrs.get('content', '')

                meta_propertyTitle = meta[meta_type]
                meta_split_info = meta_propertyTitle.split(":", 1)
                if len(meta_split_info) == 2:
                    meta_origin, meta_feature = meta_split_info
                    if meta_origin not in metadata['properties']:
                        metadata['properties'][meta_origin] = {}

                    metadata['properties'][meta_origin][meta_feature] = meta.attrs.get("content", "")

        return metadata


class GooseExtractor(HTMLExtractor):
    def __init__(self, **goose_config):
        if goose_config:
            self.extractor = Goose(**goose_config)
        else:
            self.extractor = Goose()

        self.goose_config = goose_config.get('config', {}) if goose_config else {}
        self.article = None

    def extract(self, url=None, raw_html=None, default_lang='es', cleanse=False):
        """
        Code by Mike Ogren CACI Inc.,
        Code to extract content and meta_tags by fetching URL or from HTML string
        url: URL to fetch , takes precedence when raw_html argument is also present
        raw_html: HTML string to parse
        default_lang: 'es', language to use by default (config required by Goose)
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
            self.soup_parser = BeautifulSoup(raw_html)
            content_lang = self.guess_language(raw_html)
            if not content_lang:
                content_lang = default_lang
            msg['url_language'] = content_lang
            raw_html = decode(raw_html, msg['encoding'])
            msg['metadata'] = self.get_metadata(raw_html)
        else:
            raise NotImplementedError

        goose_config = {
            'use_meta_language': False,
            'target_language': content_lang
        }
        goose_config.update(self.goose_config)

        #Special Goose requirements for Arabic, Chinese and some more special languages
        if content_lang == "ar":
            goose_config['stop_words_class'] = StopWordsArabic
        elif content_lang == "zh":
            goose_config['stop_words_class'] = StopWordsChinese
        elif content_lang == 'ko':
            goose_config['stop_words_class'] = StopWordsKorean
 
        self.article = Goose(config=goose_config).extract(raw_html=raw_html)
        if cleanse:
            msg["content"] = self.cleanse() if self.article else None
        else:
            msg["content"] = self.article.cleaned_text if self.article else None
        msg['raw_html'] = raw_html
        return msg

    def guess_language(self, resp):
        try:
            article = self.extractor.extract(raw_html=resp)
            lang = article.meta_lang

            if lang is None or lang == "en":
                lang = detect_lang(self.soup_parser.getText())

            if lang in ("sp", "ca"):
                lang = "es"

            return lang
        except KeyError, e:
            log.error("Error detecting language %s" % str(e))
            return None
    
    def cleanse(self):
        cleansed_text = self.article.cleaned_text
        if cleansed_text:
            for p in reversed(cleansed_text.split("\n")):
                if len(p.split()) < 10:
                    if not p.strip():
                        continue

                    replace_str = p
                    log.info("removed '%s' from text" % replace_str)
                    cleansed_text = cleansed_text.replace(replace_str, "")
                else:
                    break
        return cleansed_text

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

    def extract(self, urllist, urltag=None, urlContentKey='urlcontent', **extractor_config):
        unwrap = False
        if isinstance(urllist, basestring):
            urllist = [urllist]
            unwrap = True

        url_content = Parallel(n_jobs=self.n_jobs, backend='threading')(delayed(urlextract)(((url[urltag] if urltag else url)), self.extractor, **extractor_config) for url in urllist)
        if urltag:
            for index, l in enumerate(url_content):
                urllist[index][urlContentKey] = l
            url_content = urllist

        if unwrap:
            url_content = url_content[0]

        return url_content


def urlextract(url, extractor=HTMLExtractor, **extractor_config):
    try:
        if extractor_config:
            return extractor(**extractor_config).extract(url=url)
        else:
            return extractor().extract(url=url)
    except Exception, e:
        return {'error': str(e)}
