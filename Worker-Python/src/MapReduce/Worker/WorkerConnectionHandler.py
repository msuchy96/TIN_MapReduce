import socket
from multiprocessing import Lock

from src.MapReduce.Worker import Utilities
from src.MapReduce.WorkerServices.MapReduceWorker import Iface
from src.MapReduce.WorkerServices.ttypes import InvalidState, ClientListeningInfo

'''
Klasa implementujaca funkcje, ktore mozna wykonac zdalnie poprzez Thrifta na Workerze
Sluzy klasie WorkerServerThriftConnection jako obsluga metod serwera Thrift
Instancje ten klasy posiada Worker, i ta instancja jest przekazywana serwerowi do obslugi  
Przechowuje wszystkie dane, otrzymywane od Mastera
'''

class ContentNotExistException(Exception):
    pass


class WorkerConnectionHandler(Iface):
    def __init__(self, root_fs):
        self.root_fs = root_fs
        self.data_file_name = None
        self.map_file_name = None
        self.reduce_file_name = None
        self.worker_list = None
        self.pair_collector = PairCollector(root_fs)

        self.map_request = False
        self.reduce_request = False
        self.map_done = False
        self.reduce_done = False

        #self.map_pairs_to_process = []
        self.lock = Lock()

    '''
    Gettery
    '''
    def mapPath(self):
        if self.map_file_name is None:
            raise ContentNotExistException("WorkerConnectionHandler: Try to get map script path when doesn't exist")
        return str(self.root_fs) + str(self.map_file_name)

    def reducePath(self):
        if self.reduce_file_name is None:
            raise ContentNotExistException("WorkerConnectionHandler: Try to get reduce script path when doesn't exist")
        return str(self.root_fs) + str(self.reduce_file_name)

    def mapDataFilePath(self):
        if self.data_file_name is None:
            raise ContentNotExistException("WorkerConnectionHandler: Try to get data file path when doesn't exist")
        return str(self.root_fs) + str(self.data_file_name)

    def reduceDataFilePath(self):
        return str(self.root_fs) + self.getFileCollectorPath() #bo tam skladujemy pary do pliku

    def workerList(self):
        print(self.worker_list)
        return self.worker_list

    def getFileCollectorPath(self):
        return self.pair_collector.pair_file_name

    '''
    Funkcje prywatne - nie uzywac poza klasa
    '''
    def parseWorkerList(self, worker_list):

        parsed_worker_list = []
        # zamiana kolejnosci bajtow z internetowej na lokalna
        for worker in worker_list:
            ip = worker.ip
            port = worker.port
            if ip > 0:
                parsed_worker_list.append(ClientListeningInfo(ip=socket.ntohl(ip), port=port))
            else:
                parsed_worker_list.append(ClientListeningInfo(ip=socket.ntohl(ip + 1 + 0xFFFFFFFF) , port= port ) )
            #TODO - brzydka konwersja z int na uint
        return parsed_worker_list


    '''
    Obsluga metod definiowanych w protokole Thrift
    '''
    def AssignWork(self, dataFileName, mapFileName, reduceFileName, workersList):
        self.lock.acquire()
        try:
            self.data_file_name = dataFileName
            self.map_file_name = mapFileName
            self.reduce_file_name = reduceFileName
            self.worker_list = self.parseWorkerList(workersList)

        except Exception as e:
            print(str(e))
            return False
        finally:
            self.lock.release()
        return True

    def StartMap(self):

        #TODO tu powinna byc blokada, ale jezeli bedzie to wywolywal tylko master to bedzie git
        if  self.map_done or self.map_request:
            raise InvalidState("Try to execute map twice")

        if self.reduce_done or self.reduce_request:
            raise InvalidState("Try to execute map after Reduce")

        self.map_request = True
        return True

    def StartReduce(self):
        # TODO tu powinna byc blokada, ale jezeli bedzie to wywolywal tylko master to bedzie git

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
        self.lock.acquire()
        try:
            for pair in pairs:
               self.pair_collector.savePair(pair)

        except Exception as e:
            print(str(e))
            return False
        finally:
            self.lock.release()
        return True


    '''
    Metody resetujace flagi i sciezki w klasie
    '''
    def resetAll(self):
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
    '''
    Zwracaja flagi
    '''
    def isMapRequested(self):
        return self.map_request

    def isReduceRequested(self):
        return self.reduce_request

    '''
    Ustawiaja flagi
    '''
    def registerMapEnd(self):
        self.map_done = True

    def registerReduceEnd(self):
        self.reduce_done = True

    """
    Zamyka plik pair collectora - plik, ktory uzywa do zapisywania par ktore przychodza
    """
    def closeMapDataFile(self):
        self.pair_collector.closeFile()



class PairCollector():

    def __init__(self, root_fs):
        self.pair_file_name = "our_pairs.txt"
        self.pair_file = open(file=root_fs + self.pair_file_name, mode=u'w', buffering=1)

    def savePair(self, pair):

        pair_str = Utilities.PairParser.parseKeyValueToString(pair.key, pair.value)
        try:
            count = pair.quantity
            if count is None:
                count = 1 #TODO zlokalizowac czemu czasami jest None
            for i in range(int(count)):
                self.pair_file.write(pair_str + '\n')
        except Exception as e:
            print("Zachowuje pare dostarczona przez innego workera")
            print(e)

    def closeFile(self):
        self.pair_file.close()
