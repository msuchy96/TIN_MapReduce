from _ast import Not
from logging import Logger

import logging

from src.MapReduce.Worker.ExternalProcess import MapProcess
from src.MapReduce.Worker.PairManager import PairManager
from src.MapReduce.Worker.States import InterruptingStates, MainStates, State

from src.MapReduce.Worker import ConfigReader, WorkerConnectionHandler, MulticastListener, ThriftClients

from src.MapReduce.Worker.MulticastListener import MasterMulticastListener
from src.MapReduce.Worker.States.MainStates import WaitingForMasterState
from src.MapReduce.Worker.ThriftServers import WorkerServer

OUR_IP = "192.168.20.197"
WORKER_SERVER_PORT = 13000
MASTER_IP = "192.168.20.191"
MASTER_SERVER_PORT = 12000

MASTER_MCAST_GROUP = "224.0.0.123"
MASTER_MCAST_DES_PORT = 9090

AFTER_MAP_DATA_PATH = "map_reduce_sshfs/after_map.txt"

class EndOfTestException(Exception):
    pass


class Worker:
    def __init__(self):
        self.state = WaitingForMasterState(self)
        self.config_reader = ConfigReader.ConfigReader() #TODO zainicjalizowac
        self.worker_con_handler = WorkerConnectionHandler.WorkerConnectionHandler()
        self.worker_server = WorkerServer(my_ip=OUR_IP, my_port=WORKER_SERVER_PORT, handler=self.worker_con_handler)
        self.master_client = \
            ThriftClients.MasterServiceClientConnection()
        self.master_multicast = MasterMulticastListener(mcast_group=MASTER_MCAST_GROUP, my_port=MASTER_MCAST_DES_PORT)
        self.master_ip = None# MASTER_IP
        self.master_server_port = MASTER_SERVER_PORT
        self.logger = logging
        self.logging_init()
        self.map_proc = None
        self.after_map_data = AFTER_MAP_DATA_PATH
        self.pair_manager = PairManager()

    def runPairManager(self):
        self.infoLog("Run Pair Manager")

    def isPairsSent(self):
        return self.pair_manager.isPairsSent()

    def mapPath(self):
        return self.worker_con_handler.mapPath()

    def dataToMapPath(self):
        return self.worker_con_handler.dataFilePath()

    def afterMapDataPath(self):
        return self.after_map_data

    def runMapProcess(self, map_path, data_path, output_path):
        try:
            self.map_proc = MapProcess(map_path, data_path, output_path)
            self.map_proc.openInputFile()
            self.map_proc.openOutputFile()
            self.map_proc.runProcess()
        except Exception as e:
            self.fatalError("Cannot run map process !")
            raise e

    def logging_init(self):
        logging.basicConfig(level=logging.DEBUG)
        #self.logger = logging.getLogger("root")
        #self.logger.setLevel(logging.DEBUG)

    def infoLog(self, msg):
        self.logger.info(msg)

    def warningLog(self,msg):
        self.logger.warning(msg)

    def debugLog(self, msg):
        self.logger.debug(msg)

    def fatalError(self, msg):
        self.logger.critical(msg)

    def runWorker(self, testing=False):
        if not testing:
            try:
                self.state.handleState()
            except State.ChangingStateException as e:
                self.state = MainStates.state_map[str(e)](self)

        else:
            try:
                self.state.handleState()
            except State.ChangingStateException as e:
                self.closeAllResources()
                raise EndOfTestException(e)
    '''
    Tworzy polaczenie klienckie Thrift z masterem 
    na podstawie adresu ip mastera zapisanego wewnatrz klasy oraz portu, na ktorym umowiono sie komunikowac
    '''
    def createClientConnectionWithMaster(self):
        #zapisz, ze jest 1. polaczenie z masterem, uruchom sluchacza ktory bedzie patrzyl czy master zyje
        #oraz utworz polaczenie z masterem
        self.debugLog("Try to client connection with Master")
        self.master_multicast.firstConnectionActive()
        self.master_multicast.runListeningThread()
        self.debugLog("Run Multucast from Master Listener Thread")
        self.master_client.openConnection(self.master_ip, MASTER_SERVER_PORT)
        self.debugLog("Client Connection with Master opened")

    def createWorkerServer(self):
        self.worker_server.createThreadedServer()

    def startServer(self):
        self.worker_server.startServing()
        self.debugLog("Start Worker server Thread")

    def registerWorker(self):
        self.master_client.registerWorker(OUR_IP, WORKER_SERVER_PORT)
        self.debugLog("Registered on Master ")

    def readConfiguration(self):
        #raise NotImplementedError
        pass

    def saveMasterIp(self, master_ip):
        self.master_ip = master_ip

    def closeAllResources(self):
        self.master_multicast.close()

    def isMapProcesEnd(self):
        return self.map_proc.isRun()

    def isPairManagerEnd(self):
        return self.pair_manager.isPairsSent()

    def isMapRequested(self):
        return self.worker_con_handler.isMapRequested()

    def isMasterLive(self):
        return self.master_multicast.isMasterLived()

    def connectWithNewMaster(self):
        pass

