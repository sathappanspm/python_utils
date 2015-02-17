#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

import pandas as pd


class DataFrame_mod(pd.DataFrame):
    def multiapply(self, applyfns={}, newCols={}, complexfns={}):
        for col, fn in newCols.iteritems():
            self[col] = self[fn['transformCol']].apply(fn['transformFn'])

        for col, fn in applyfns.iteritems():
            self[col] = self[col].apply(fn)

        for col, fn in complexfns.iteritems():
            self[col] = fn(self)

        return self
