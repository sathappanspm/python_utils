#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"
__processor__ = "globalUtils"

from unidecode import unidecode
import sys
from datetime import datetime
import re
from math import radians, sin, cos, atan2, sqrt, degrees


dateRE = re.compile('\d\d\d\d-\d{1,2}-\d{1,2}')

OSI_FORMAT_LOC_CORRECT = {'Mexico,Distrito Federal,Ciudad De Mexico': 'Mexico,Ciudad de México,Ciudad de México',
                          'Argentina,Distrito Federal,Buenos Aires': 'Argentina,,Buenos Aires',
                          'Venezuela,Distrito Capital,Caracas': 'Venezuela,Caracas,Caracas',
                          'Colombia,Bogota,Bogota': 'Colombia,Bogota,Bogota',
                          'Ecuador,Pichincha,Quito': 'Ecuador,Pichincha,Quito',
                          'El Salvador, San Salvador, San Salvador': 'El Salvador, San Salvador, San Salvador',
                          'Paraguay,Asuncion,Asuncion': 'Paraguay,Asuncion,Asuncion',
                          'Uruguay,Montevideo,Montevideo': 'Uruguay,Montevideo,Montevideo',
                          'Hoduras,Tegucigalpa,Tegucigalpa': 'Hoduras,Tegucigalpa,Tegucigalpa',
                          'Peru,Lima,Lima': 'Peru,Lima,Lima',
                          'Brazil,Brasilia,Brasilia': 'Brazil,Brasilia,Brasilia',
                          'Chile,Metropolitana,Santiago': 'Chile,Santiago,Santiago'
                          }
GEO_CORRECTION = {}


def format_loc(loc_item):
    if len(loc_item) == 0 or loc_item.lower() == 'na' or loc_item.lower() == 'n/a':
        return '-'
    else:
        return loc_item.strip()


def format_str(s):
    if isinstance(s, str):
        return s.strip().decode('utf-8')
    if isinstance(s, unicode):
        return s.strip()
    return unicode(s).strip()


def load_geo_correction():
    with open('/home/vicky/workspace/git/embers/embersUtils/gsr_geo_correction.txt') as f:
        lines = [[j.strip().split(',') for j in l.strip().split(':')] for l in
                 f if l.strip()]
    return {(format_str(a[0][0]), format_str(a[0][1]), format_str(a[0][2])): a[1] for a in lines}


def apply_osi_correction(geoList):
    global GEO_CORRECTION
    if not GEO_CORRECTION:
        GEO_CORRECTION = load_geo_correction()
    if tuple(geoList) in GEO_CORRECTION:
        loctuple = list(GEO_CORRECTION[tuple(geoList)])
        loctuple_norm = ",".join([format_str(k).title() for k in loctuple])
        if loctuple_norm in OSI_FORMAT_LOC_CORRECT:
            return OSI_FORMAT_LOC_CORRECT[loctuple_norm].split(","), True
        return GEO_CORRECTION[tuple(geoList)], True
    return geoList, False


class STD_IO_HANDLER(object):
    """A class object to handle std_out and std_in in a similar fashion to queue write and read"""
    def __init__(self, file=None, nocaptureFlag=False):
        import json
        #import codecs
        self.jLoads = json.loads
        self.jDumps = json.dumps
        if not file:
            self.reader = sys.stdin
            self.writer = sys.stdout
        else:
            self.reader = open(file)
            self.writer = sys.stdout

        if nocaptureFlag:
            self.write = self.nocaptureWrite

    def read(self):
        jLoads = self.jLoads
        for l in self.reader:
            try:
                j = jLoads(l.decode("utf-8"))
                yield j
            except ValueError, e:
                print "Error in reading", str(e)
                continue

    def write(self, article):
        self.writer.write(self.jDumps(article,  ensure_ascii=False).encode("utf-8") + "\n")
        return

    def nocaptureWrite(self, article):
        pass

    def close(self):
        self.writer.close()
        self.reader.close()


def print_csv(data, separator=",", rowHeader='', default=0, transpose=True, colNames=None, rowNames=None, total=False,
              fwrite=sys.stdout, asboolean=False, **kwargs):
    """
    seperator: Type of seperator to be used like comma,  tab etc
    rowHeader: the name of each row, if none default is '', where i is the row number
    colNames: Names of columns
    transpose: if True, the data is stored as data[column][row] else it is data[row][col]
    """
    #print data.keys()

    if transpose:
        if not colNames:
            colNames = data.keys()

        if not rowNames:
            rowNames = []
            for k in data:
                rowNames.extend(data[k].keys())
            row_index = sorted(list(set(rowNames)))
        else:
            row_index = rowNames

        for k in colNames:
            if k not in data:
                data.setdefault(k, dict())

        header = separator.join([toStr(rowHeader)] + [toStr(k) for k in colNames])
        if total:
            header += separator + "total"

        fwrite.write(header + "\n")
        for i in row_index:
            row = [toStr(i)] + [toStr(data[k].get(i, default)) for k in colNames]
            if total:
                row += [str(sum([data[k].get(i, float(default)) for k in colNames]))]

            if asboolean:
                new_row = [row[0]]
                for v in row[1:]:
                    new_row.append(1 if v else 0)
                row = new_row
            fwrite.write(separator.join(row).encode("utf-8") + "\n")
    else:
        if not rowNames:
            row_index = sorted(data.keys())
            rowNames = row_index
        else:
            row_index = rowNames

        if not colNames:
            colNames = []
            for k in data:
                colNames.extend(data[k].keys())
            colNames = sorted(list(set(colNames)))

        header = separator.join([toStr(rowHeader)] + [toStr(k) for k in colNames])
        if total:
            header += separator + "total"

        for k in rowNames:
            data.setdefault(k, dict())

        fwrite.write(header.encode("utf-8") + "\n")
        for i in row_index:
            row = [toStr(i)] + [toStr(data[i].get(k, default)) for k in colNames]
            if total:
                row += [str(sum([data[i].get(k, float(default)) for k in colNames]))]

            if asboolean:
                new_row = [row[0]]
                for v in row[1:]:
                    new_row.append(1 if v else 0)
                row = new_row

            fwrite.write(separator.join(row).encode("utf-8") + "\n")

    return


def toDate(date):
    dt = None
    try:
        dt = datetime.strptime(re.findall(dateRE, date)[0], '%Y-%m-%d')
    except Exception, e:
        sys.stderr.write('date could not be decoded: %s, %s' % (date, e))
    return dt


def toStr(x, sep=","):
    """Creates string representation of the incmg object. If list, elements will be seperated by char sep."""
    if isinstance(x, str) or isinstance(x, unicode):
        return x
    try:
        return sep.join([str(k) for k in x])
    except:
        return str(x)


def normalize_str(s):
    """strip string of accents. Converts input string to Ascii and returns a unicode object"""
    if s is None:
        s = ""
    try:
        if isinstance(s, str):
            return unidecode(s.decode('utf-8').strip()).lower()
    except UnicodeDecodeError:
        return unidecode(s.strip()).lower()
    return unidecode(s.strip()).lower()


def calc_geodistance(coord1, coord2):
    """
    Calculate distance between two coordinates using the haversine formula
    """
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 6371  # Radius of the earth
    lat_delta = radians(lat1 - lat2)
    lon_delta = radians(lon1 - lon2)
    sine_latdelta = sin(lat_delta / 2.0)
    sine_londelta = sin(lon_delta / 2.0)
    a = sine_latdelta * sine_latdelta + cos(radians(lat1)) * cos(radians(lat2)) * sine_londelta * sine_londelta
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def calc_bearing(coord1, coord2, final=False):
    """
    Calculate the angle between the two coordinates
    """
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    if final:
        y = sin(lon1 - lon2) * cos(lat1)
        x = cos(lat2) * sin(lat1) - sin(lat2) * cos(lat1) * cos(lon1 - lon2)
        return (degrees(atan2(y, x)) + 180) % 360

    y = sin(lon2 - lon1) * cos(lat2)
    x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lon2 - lon1)
    return (degrees(atan2(y, x)) + 360) % 360


def main(args):
    pass

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    #ap.add_argument("")
    args = ap.parse_args()
    main(args)
