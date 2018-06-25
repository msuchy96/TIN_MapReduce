

"""
Bierze pary klucz-wartosc z outputu procesu, do ktorego ma referencje,
i cos z nimi robi (co i jak definiuja klasy pochodne)
Klasy tej nie powinno sie tworzyc explicite - jest tylko impelementacja pewnych wspolnych cech
"""
import ipaddress
import re
import socket
import subprocess
import threading
from subprocess import Popen
import string


from src.MapReduce.Worker import Utilities, KeyValueDictionary
from src.MapReduce.Worker.ThriftClients import WorkerServiceClient
from src.MapReduce.WorkerServices import ttypes
from src.MapReduce.WorkerServices.ttypes import InvalidState


class PairManager():
    MAX_CHAR_IN_LINE = 1024

    def __init__(self, worker_ref, thread_name):
        self.is_running = False
        self.pairs_sent = False
        self.worker_ref = worker_ref
        self.proc = None
        self.thread_name = thread_name
        self.lock = threading.Lock()
        self.thread = threading.Thread(group=None, target=self, name=self.thread_name)

    def __call__(self, *args, **kwargs):
        raise NotImplemented("Try to call thread in abstract class")

    def addMapProcRef(self, proc):
        self.proc = proc

    def getNextPair(self):
        return self.proc.stdout.readline(PairManager.MAX_CHAR_IN_LINE)

    def run(self, proc):
        self.proc = proc
        self.thread.start()

    def isPairsSent(self):
        self.lock.acquire()
        pairs_sent = self.pairs_sent
        self.lock.release()

        return not pairs_sent


class MapPairManager(PairManager):

    def __init__(self, worker_ref):
        PairManager.__init__(self, worker_ref, "Map Pair Manager Thread")
        self.workers_addr_list = []
        self.workers_connections_list = []


    def __call__(self, *args, **kwargs):
        self.lock.acquire()
        self.is_running = True
        self.lock.release()

        self.loadWorkerList()
        self.createConnectionsToWorkers()
        while 1:
            str_key_val = self.getNextPair()
            if str_key_val is '':
                break #TODO zamienic na ladniejsze sprawdzanie czy w stdout cos jeszcze zostalo
            key, value = Utilities.PairParser.parseStringToKeyValue(str_key_val)
            hashed_key = Utilities.Hash.getHash(key, len(self.workers_addr_list))
            self.sendKeyValueToWorker(hashed_key, key, value, 1)

        self.closeConnectionsAndFile()

        self.lock.acquire()
        self.is_running = True
        self.pairs_sent = True
        self.lock.release()

    def sendKeyValueToWorker(self, worker_num, key, value, quantity):
        addresser = self.workers_connections_list[worker_num]
        try:
            pair = ttypes.KeyValueEntity(key, value, quantity)
            print(pair)
            addresser.RegisterMapPair( [ pair ] )
        except InvalidState as e:
            self.worker_ref.fatalError("Cannot send key-value entity to another worker")
            raise e

    def createConnectionsToWorkers(self):

        for worker_addr in self.workers_addr_list:
            try:
                connection = WorkerServiceClient()
                connection.openConnection(str(ipaddress.ip_address(worker_addr.ip)), int(worker_addr.port))
                self.workers_connections_list.append(connection)
            except Exception as e:
                self.worker_ref.fatalError("Can't connect to other worker!")
                self.worker_ref.fatalError("Ip is:" + str(ipaddress.IPv4Address(ipaddress.ip_address(worker_addr.ip))) +
                                           "Port is " + str(ipaddress.IPv4Address(worker_addr.port)))

    def closeConnectionsAndFile(self):
        for worker_con in self.workers_connections_list:
            worker_con.closeConnection()

    def loadWorkerList(self):
        self.workers_addr_list = self.worker_ref.getWorkerList()


class ReducePairManager(PairManager):
    def __init__(self, worker_ref, after_map_file, reduce_script_path):
        PairManager.__init__(self, worker_ref, "Reduce Pair Manager Thread")
        self.dict = KeyValueDictionary.PairDictionary(after_map_file)
        self.reduce_script_path = reduce_script_path

    def __call__(self, *args, **kwargs):
        self.lock.acquire()
        self.is_running = True
        self.lock.release()

        self.dict.openFile()
        self.dict.loadPairsToDictionary()
        reduce_proc = None

        for key in self.dict.dictionary:
            #sklej wszystkie wartosci w jeden string
            bucket = self.dict.getValuesArray(key)
            str = Utilities.PairParser.getLinedStringFromArray(bucket)
            #uruchom proces , przekaz mu stringa i przechwyc wynik
            reduce_proc = Popen(args=["python3", self.reduce_script_path],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE,  universal_newlines=True)
            stdout, stderr = reduce_proc.communicate(str)
            self.sendKeyValueToMaster(key, stdout.replace('\n', '') )#stdout to value zwrocona przez skrypt reduce

        self.lock.acquire()
        self.pairs_sent = True
        self.is_running = False
        self.lock.release()

    def sendKeyValueToMaster(self, key, value):
        self.worker_ref.registerResultIntoMaster(key, value)