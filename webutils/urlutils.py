#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from urlparse import urlparse
import os
import re

urlre = re.compile("(?P<url>https?://[^\s]+)", re.I|re.U)

with open(os.path.join(os.path.dirname(__file__), "effective_tld_names.dat.txt")) as tldFile:
    tlds = [line.strip().decode("utf-8") for line in tldFile if line[0] not in "/\n"]


class DomainParts(object):
    def __init__(self, domain_parts, tld):
        self.domain = None
        self.subdomains = None
        self.tld = tld
        if domain_parts:
            self.domain = domain_parts[-1]
            if len(domain_parts) > 1:
                self.subdomains = domain_parts[:-1]


def findUrl(text):
    s = urlre.search(text)
    if s:
        return s.group("url")
    else:
        return None


def get_domain_parts(url):
    urlElements = urlparse(url).hostname.split('.')
    # urlElements = ["abcde","co","uk"]
    for i in range(-len(urlElements),0):
        lastIElements = urlElements[i:]
        #    i=-3: ["abcde","co","uk"]
        #    i=-2: ["co","uk"]
        #    i=-1: ["uk"] etc

        candidate = ".".join(lastIElements) # abcde.co.uk, co.uk, uk
        wildcardCandidate = ".".join(["*"]+lastIElements[1:]) # *.co.uk, *.uk, *
        exceptionCandidate = "!"+candidate

        # match tlds:
        if (exceptionCandidate in tlds):
            return ".".join(urlElements[i:])
        if (candidate in tlds or wildcardCandidate in tlds):
            return DomainParts(urlElements[:i], '.'.join(urlElements[i:]))
            # returns ["abcde"]

    raise ValueError("Domain not in global list of TLDs")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument()
    args = ap.parse_args()
    main(args)
