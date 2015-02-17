#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

import json
import glob


class mjsonLoader(object):
    @staticmethod
    def _load(fileObj, encoding):
        with open(fileObj) as fin:
            return [json.loads(l.strip(), encoding) for l in fin if l.strip()]

    @staticmethod
    def load(filePattern, encoding='utf-8'):
        fileList = glob.glob(filePattern)
        mjson = []
        for l in fileList:
            mjson.extend(mjsonLoader._load(l, encoding))

        return mjson

    @staticmethod
    def loads(s, encoding='utf-8'):
        return [json.loads(l.strip()) for l in s.split('\n') if l.strip()]

    @staticmethod
    def dump(jsObj, filename, ensure_ascii=False):
        with open(filename, "w") as fout:
            if isinstance(jsObj, list):
                for j in jsObj:
                    fout.write(json.dumps(j, ensure_ascii=ensure_ascii).encode('utf-8') + "\n")
            else:
                fout.write(json.dumps(jsObj, ensure_ascii=False).encode("utf-8"))
