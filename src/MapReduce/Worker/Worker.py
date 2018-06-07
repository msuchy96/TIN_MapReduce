from _ast import Not
from logging import Logger

import logging

from src.MapReduce.Worker.States import InterruptingStates, MainStates, State

from src.MapReduce.Worker import ConfigReader, WorkerConnectionHandler, MulticastListener, ThriftClients

from src.MapReduce.Worker.MulticastListener import MasterMulticastListener
from src.MapReduce.Worker.States.MainStates import WaitingForMasterState
from src.MapReduce.Worker.ThriftServers import WorkerServer

OUR_IP = "192.168.20.197"
WORKER_SERVER_PORT = 13000
MASTER_IP = "192.168.20.163"
MASTER_SERVER_PORT = 12000

MASTER_MCAST_GROUP = "224.0.0.123"
MASTER_MCAST_DES_PORT = 9090



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

    def logging_init(self):
        logging.basicConfig(level=logging.DEBUG)
        #self.logger = logging.getLogger("WORKER LOG")
        #self.logger.setLevel(logging.DEBUG)

    def infoLog(self, msg):
        logging.info(msg)

    def warningLog(self,msg):
        logging.warning(msg)

    def debugLog(self, msg):
        logging.debug(msg)

    def fatalError(self, msg):
        logging.critical(msg)

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
        self.master_multicast.firstConnectionActive()
        self.master_multicast.runListeningThread()
        self.master_client.openConnection(self.master_ip, MASTER_SERVER_PORT)

    def createWorkerServer(self):
        self.worker_server.createThreadedServer()

    def registerWorker(self):
        self.master_client.registerWorker(OUR_IP, WORKER_SERVER_PORT)

    def readConfiguration(self):
        #raise NotImplementedError
        pass

    def saveMasterIp(self, master_ip):
        self.master_ip = master_ip

    def closeAllResources(self):
        self.master_multicast.close()

    def isMapRequested(self):
        return self.worker_con_handler.isMapRequested()

    def isMasterLive(self):
        return self.master_multicast.isMasterLived()

    def connectWithNewMaster(self):
        pass

