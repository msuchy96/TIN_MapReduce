from multiprocessing import Lock

from src.MapReduce.WorkerServices.MapReduceWorker import Iface
from src.MapReduce.WorkerServices.ttypes import InvalidState

'''
Klasa implementujaca funkcje, ktore mozna wykonac zdalnie poprzez Thrifta na Workerze
Sluzy klasie WorkerServerThriftConnection
'''


class WorkerConnectionHandler(Iface):
    def __init__(self):
        self.data_file_name = ""
        self.map_file_name = ""
        self.reduce_file_name = ""
        self.worker_list = None

        self.map_request = False
        self.reduce_request = False
        self.map_done = False
        self.reduce_done = False

        self.map_pairs_to_process = []
#        self.lock = Lock()

    def parseWorkerList(self, workerListString):
        list = [1, 2, 3]
        return list

    def AssignWork(self, dataFileName, mapFileName, reduceFileName, workersList):
 #       self.lock.acquire()
        try:
            self.data_file_name = dataFileName
            self.map_file_name = mapFileName
            self.reduce_file_name = reduceFileName
            self.worker_list = self.parseWorkerList(workersList)

        except Exception as e:
            print(str(e))
            return False
  #      finally:
  #          self.lock.release()
        return True

    def StartMap(self):
        if  self.map_done or self.map_request:
            raise InvalidState("Try to execute map twice")

        if self.reduce_done or self.reduce_request:
            raise InvalidState("Try to execute map after Reduce")

        self.map_request = True
        return True

    def StartReduce(self):
        if not self.map_request:
            raise InvalidState("Map step didn't run yet")

        if not self.map_done and self.map_request:
            raise InvalidState("Map step haven't done yet")

        if self.reduce_request or self.reduce_done:
            raise InvalidState("Try to execute reduce twice")

        self.reduce_request = True
        return True

    def Ping(self):
        return 1

    def RegisterMapPair(self, pairs):
        try:
            self.map_pairs_to_process += pairs
        except Exception as e:
            print(e)
            return False
  #      finally:

        return True

    def resetWorker(self):
        self.resetFlags()
        self.resetWorkInfo()

    def resetWorkInfo(self):
        self.data_file_name = None
        self.map_file_name = None
        self.reduce_file_name = None
        self.worker_list = None

    def resetFlags(self):
        self.map_request = False
        self.map_done = False
        self.reduce_request = False
        self.reduce_done = False

    def isMapRequested(self):
        return self.map_request

    def registerMapEnd(self):
        self.map_done = True

    def isReduceRequested(self):
        return self.reduce_request

    def registerReduceEnd(self):
        self.reduce_done = True