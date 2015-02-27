#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

import threading
import Queue
import json


def encode(s, encoding="utf-8"):
    try:
        return s.encode(encoding)
    except:
        return s


class ResultsWriter(object):
    def __init__(self):
        self.res = []

    def write(self, data):
        self.res.append(data)
        #yield data


class jsonWriter(ResultsWriter):
    def __init__(self, outfile, sep="\n"):
        self.handler = open(outfile, "w")
        self.sep = sep

    def __enter__(self):
        return self
    
    def write(self, data):
        try:
            self.handler.write(encode(json.dumps(data, ensure_ascii=False)) + self.sep)
        except:
            self.handler.write(json.dumps(data) + self.sep)

    def close(self):
        self.handler.close()

    def __exit__(self, typ, value, traceback):
        self.close()


class Processor(threading.Thread):
    def __init__(self, data, process_func):
        super(self.__class__, self).__init__()
        self.process_func = process_func
        self.data = data
        self.result = None

    def run(self):
        self.result = self.process_func(self.data)
    
    def get_result(self):
        return self.result


class Reducer(threading.Thread):
    def __init__(self, msg_q, resultsWriter, data_len):
        super(Reducer, self).__init__()
        self.queue = msg_q
        self.size = data_len
        self.resultsWriter = resultsWriter

    def run(self):
        processed = 0
        while processed < self.size:
            worker = self.queue.get()
            worker.join()
            self.resultsWriter.write(worker.get_result())
            self.queue.task_done()
            processed += 1

        self.queue.join()


class Mapper(threading.Thread):
    def __init__(self, msg_q, worker, dataIter, daemon=True):
        super(Mapper, self).__init__()
        self.queue = msg_q
        self.worker = worker
        self.dataiter = dataIter
        self.daemon = daemon
        self.datalen = 0

    def run(self):
        for msg in self.dataiter:
            w = Processor(msg, self.worker)
            w.daemon = self.daemon
            w.start()
            self.queue.put(w, True)
        return



def parallel_run(func, dataiter, n_jobs=10, mapper=Mapper, reducer=Reducer, writer=ResultsWriter()):
    msg_q = Queue.Queue(n_jobs)
    map_obj = mapper(msg_q, func, dataiter)
    map_obj.start()
    red_obj = reducer(msg_q, writer, len(dataiter))
    red_obj.start()

    map_obj.join()
    red_obj.join()
    return writer

