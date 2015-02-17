#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

#Not to be used as main program..Only to be used via import

import json
from pandasutils import DataFrame_mod
from dateutil.parser import parse
import pandas as pd
import numpy as np
#from sklearn.neighbors import KernelDensity
from scipy.stats import gaussian_kde
#import matplotlib.pyplot as plt
import shelve
from embers.geocode import Geo


cntrySet = set(["Argentina", "Brazil", "Chile", "Colombia", "Mexico", "Paraguay",
                "Ecuador", "Venezuela", "El Salvador", "Uruguay"])

CAPITAL_CITIES = ('Mexico,Distrito Federal,Ciudad de México', 'Argentina,Distrito Federal,Buenos Aires',
                  'Venezuela,Distrito Capital,Caracas', 'Colombia,Bogotá,Bogotá', 'Ecuador,Pichincha,Quito',
                  'El Salvador,San Salvador,San Salvador', 'Paraguay,Asunción,Asunción', 'Uruguay,Montevideo,Montevideo',
                  'Honduras,Tegucigalpa,Tegucigalpa', 'Peru,Lima,Lima', 'Brazil,Distrito Federal,Brasilia',
                  'Chile,Metropolitana,Santiago', 'Mexico,Distrito Federal,Ciudad de Mexico', 'Brazil,Distrito Federal,Brasília')

CAPITAL_CITIES_OSI_CORRECTION = (u'Mexico,Ciudad de México,Ciudad de México', u'Argentina,-,Buenos Aires',
                                 u'Venezuela,Caracas,Caracas', u'Colombia,Bogotá,Bogotá', u'Ecuador,Pichincha,Quito',
                                 u'El Salvador,San Salvador,San Salvador', u'Paraguay,Asunción,Asunción',
                                 u'Uruguay,Montevideo,Montevideo', u'Honduras,Tegucigalpa,Tegucigalpa',
                                 u'Peru,Lima,Lima', u'Brazil,Brasília,Brasília',
                                 u'Chile,Santiago,Santiago', u'Mexico,Ciudad de México,Ciudad de México',
                                 u'Brazil,Brasília,Brasília')


def decode(s, coding="utf-8"):
    try:
        return s.decode("utf-8")
    except:
        return s


OSI_FORMAT_LOC_CORRECT = dict(zip([l.decode("utf-8") for l in CAPITAL_CITIES],
                                  [l for l in CAPITAL_CITIES_OSI_CORRECTION]))

OSI_FORMAT_LOC_CORRECT_REVERSE = {v.lower(): k for k, v in OSI_FORMAT_LOC_CORRECT.iteritems()}


def encode(s, coding="utf-8"):
    try:
        return s.encode("utf-8")
    except:
        return str(s)


def plot_kde(n, ax=None):
    #kde = KernelDensity(bandwidth=0.2, kernel='gaussian')
    kde = gaussian_kde(bandwidth=0.2)
    #kde.fit(n[:, np.newaxis])
    kde.evaluate(n[:, np.newaxis])
    max_limit = np.max(n)
    min_limit = np.min(n)
    if not ax:
        fig = plt.figure()
        ax = fig.add_subplot(111)

    x = np.linspace(min_limit, max_limit, 10000)
    y = np.exp(kde.score_samples(x[:, np.newaxis]))
    ax.plot(x, y)
    ax.set_xlabel("QS")
    ax.set_ylabel("Density")
    return ax


def reverse_osicorrection(loctuple):
    """docstring for rev"""
    locstr = ",".join([loctuple[0].strip() if loctuple[0].strip() else "-",
             loctuple[1].strip() if loctuple[1].strip() else "-",
             loctuple[2].strip() if loctuple[2].strip() else "-"])

    if locstr.lower() in OSI_FORMAT_LOC_CORRECT_REVERSE:
        return OSI_FORMAT_LOC_CORRECT_REVERSE[locstr.lower()].split(",")

    co, st, ci = loctuple
    return co, st, ci


class WarningParser(object):
    def __init__(self, transforms=None, createCols=None, complex_fns={}):
        self.et_cls = ("011", "012", "013", "014", "015", "016")
        self.pop_cls = (u'Business', u'Media', u'Medical', u'Legal', u'General Population',
                        u'Refugees/Displaced', u'Ethnic', u'Agricultural', u'Labor', u'Religious',
                        u'Education')
        self.viol_cls = ("1", "2")
        if transforms:
            self.transforms = transforms
        else:
            self.transforms = {'eventDate': pd.to_datetime(lambda x: parse(x[:10])), 'date': pd.to_datetime(lambda x: parse(x[:10]))}

        if createCols:
            self.createCols = createCols
        else:
            self.createCols = {'month': {'transformCol': 'eventDate', 'transformFn': lambda x: x[:7]},
                               'country': {'transformCol': 'location', 'transformFn': lambda x: x[0]},
                               'model_short': {'transformCol': 'model', 'transformFn': self.get_model}}

        if complex_fns:
            self.complex_fns = complex_fns
        else:
            self.complex_fns = {"reportingDelay": lambda x: (x['date'] - x['eventDate']) / np.timedelta64(1, 'D')}

        self.seen = shelve.open('persistent_shelve.db')
        self.embersgeo = None

    def parse(self, gsrObj, geo=False):
        if geo:
            self.embersgeo = Geo()

        if isinstance(gsrObj, file):
            gsr = [self._formatcheck(json.loads(l), geo) for l in gsrObj if l.strip()]

        elif isinstance(gsrObj, basestring):
            with open(gsrObj) as gfile:
                gsr = [self._formatcheck(json.loads(l), geo) for l in gfile if l.strip()]

        elif isinstance(gsrObj, list):
            gsr = [self._formatcheck(j, geo) for j in gsrObj]

        else:
            raise NotImplementedError

        gsr_df = self._dfmap(gsr)
        return gsr_df

    def _dfmap(self, gsr):
        return DataFrame_mod(gsr).multiapply(applyfns=self.transforms, newCols=self.createCols, complexfns=self.complex_fns)

    def _formatcheck(self, j, geo=False):
        if "classification" not in j:
            try:
                if len(j["eventType"]) < 4:
                    j["eventType"] += "1"
                j["classification"] = {"eventType": {k: 0.0 for k in self.et_cls},
                                       "population": {k: 0.0 for k in self.pop_cls},
                                       "violence": {k: 0.0 for k in self.viol_cls}
                                       }

                j["classification"]["eventType"][j["eventType"][:3]] = 1.0
                j["classification"]["violence"][j["eventType"][3]] = 1.0
                j["classification"]["population"][j["population"]] = 1.0
            except:
                pass
        if isinstance(j["classification"], basestring):
            j["classification"] = json.loads(j["classification"])

        if isinstance(j["classification"], list):
            j["classification"] = j["classification"][0]

        if "matched_gsr" in j and isinstance(j["matched_gsr"], basestring):
            j["matched_gsr"] = json.loads(j["matched_gsr"])

        if "match_score" in j and isinstance(j["match_score"], basestring):
            j["match_score"] = json.loads(j["match_score"])

        #all empty location fields have to be expressed using '-' instead of ''
        if 'location' in j:
            for l in j['location']:
                if l.strip() == '':
                    l = '-'

        if geo:
            j['locInfo'] = self.get_locInfo(j['location'])
            j['coordinates'] = [j['locInfo']['latitude'], j['locInfo']['longitude']] if j['locInfo'] else [None, None]
        return j

    def is_citylevel(self, loc):
        """Determine if city level info is present in location"""
        if loc[2] != "" and loc[2] != "-":
            return True

        return False

    def get_locInfo(self, loctuple, canonicalLT=None):
        lstr = encode(','.join(loctuple))
        if lstr in self.seen:
            return self.seen[lstr]

        loc_headers = ("city", "country", "state", "admin2", "admin3", "population_size",
                       "latitude", "longitude", "id", "freq")
        if not self.is_citylevel(loctuple):
            return None

        if not canonicalLT:
            co, st, ci = reverse_osicorrection(loctuple)
            #if not self.is_citylevel(loctuple):
            #    canonicalLT, aliasLT = self.embersgeo.best_guess(ci, co, st)
            canonicalLT, aliasLT = self.embersgeo.best_guess_city(ci, co, st)

        if canonicalLT:
            try:
                canonicalLT_corrected = [decode(l) for l in canonicalLT[0]]
                canonicalLT[0] = canonicalLT_corrected
            except:
                pass
            ldict = dict(zip(loc_headers, canonicalLT[0]))
            self.seen[lstr] = ldict
            return ldict

        if aliasLT:
            try:
                aliasLT_corrected = [decode(l) for l in aliasLT[0]]
                aliasLT[0] = aliasLT_corrected
            except:
                pass

            ldict = dict(zip(loc_headers, aliasLT[0]))
            self.seen[lstr] = ldict
            return ldict

        return None

    def get_model(self, name):
        """get model _short name"""
        nl = name.lower()
        if "dynamic query" in nl:
            return "dqe"

        if "LASSO" in name:
            return "lasso"

        if "baserate" in nl:
            return "br"

        if "planned" in nl:
            return "pp"

        if "mle" in nl:
            return "mle"

        if "civil unrest fast-spatial-scan" in nl:
            return "ss"

        if "locReWriter" in nl:
            return "lw"

        return nl


def combine_predictions(m_df):
    m_df['LS'] = m_df['match_score'].apply(lambda x: x['warning_score']['location'] if isinstance(x, dict) else 0.0)
    m_df['DS'] = m_df['match_score'].apply(lambda x: x['warning_score']['date'] if isinstance(x, dict) else 0.0)
    m_df['QS'] = (m_df['LS'] + m_df['DS']) * 2
    m_df['match_score'] = m_df['match_score'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else
                                                    '{"warning_score": {"location": 0.0, "date": 0.0}}')
    m_df['classification'] = m_df['classification'].apply(lambda x: json.dumps(x) if isinstance(x, dict)
                                                          else "{}")

    return combine_groups(m_df, 'mitreId')


def combine_groups(df, groupbyAttrb):
    m_grps = df.groupby(groupbyAttrb)

    def transFunc(s):
        if s.dtype == float:
            return s.mean()
        return s.iloc[0]

    m_combined = m_grps.agg(transFunc)
    m_combined.reset_index(inplace=True)
    return m_combined
