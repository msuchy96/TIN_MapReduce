import socket
from _ast import Not
from ipaddress import ip_address
from logging import Logger

import logging

from src.MapReduce.Worker.ExternalProcess import MapProcess, ReduceProcess
from src.MapReduce.Worker.PairManager import PairManager, MapPairManager, ReducePairManager
from src.MapReduce.Worker.States import InterruptingStates, MainStates, State

from src.MapReduce.Worker import ConfigReader, WorkerConnectionHandler, MulticastListener, ThriftClients

from src.MapReduce.Worker.MulticastListener import MasterMulticastListener
from src.MapReduce.Worker.States.MainStates import WaitingForMasterState
from src.MapReduce.Worker.ThriftServers import WorkerServer

OUR_IP = "192.168.20.197"
WORKER_SERVER_PORT = 13000
#MASTER_IP = "192.168.43.107"
MASTER_SERVER_PORT = 12000

MASTER_MCAST_GROUP = "224.0.0.123"
MASTER_MCAST_DES_PORT = 9090


class EndOfTestException(Exception):
    pass

"""
Klasa reprezentuje caly program Workera
Nie decyduje o tym, co ma byc w danej chwili robione - od tego jest pole skladowe Workera, "state". 
To stan decyduje, co ma byc robione

Zamiast tego posredniczy w wywolywaniu przez stany metod klas, ktore posiada

"""
class Worker:
    def __init__(self):
        self.state = WaitingForMasterState(self)
        self.config_reader = ConfigReader.ConfigReader()
        self.worker_con_handler = WorkerConnectionHandler.WorkerConnectionHandler(self.config_reader.getRootDirectory())
        self.worker_server = WorkerServer(my_ip=self.config_reader.getWorkerIp(), my_port=self.config_reader.getWorkerServerPort()
                                          , handler=self.worker_con_handler)
        self.master_client = \
            ThriftClients.MasterServiceClientConnection()
        self.master_multicast = MasterMulticastListener(mcast_group=self.config_reader.getMasterMulticastIp(),
                                                        my_port=self.config_reader.getWorkerMulticastPort())
        self.master_ip = None
        self.master_server_port = self.config_reader.getMasterPort()
        self.logger = logging
        self.logging_init()
        self.map_proc = None
        self.map_pair_manager = None
        self.reduce_pair_manager = None


    '''
    Gettery zwracajace wymagane przez podzespoly elementy
    '''
    def getOutIp(self):
        return (int(ip_address(socket.inet_aton(OUR_IP)))) - 1 - 0xFFFFFFFF

    def getOurWorkerServerPort(self):
        return self.config_reader.getWorkerServerPort()

    def getWorkerList(self):
        return self.worker_con_handler.workerList()


    """
    Gettery zwracajace sciezki do skryptow map, reduce i danych do nich
    """

    def mapPath(self):
        return self.worker_con_handler.mapPath()

    def reducePath(self):
        return self.worker_con_handler.reducePath()

    def mapDataFilePath(self):
        return self.worker_con_handler.mapDataFilePath()

    def dataToReducePath(self):
        return self.worker_con_handler.reduceDataFilePath()

    """
    Gettery zwracajace flagi, mowiace o trwaniu/zakonczeniu roznych procesow/watkow i zadan
    """

    def isMapProcessEnd(self):
        return self.map_proc.isRun()

    def isMapPairManagerEnd(self):
        return not self.map_pair_manager.isPairsSent()

    def isReducePairManagerEnd(self):
        return not self.reduce_pair_manager.isPairsSent()

    def isMapRequested(self):
        return self.worker_con_handler.isMapRequested()

    def isReduceRequested(self):
        return self.worker_con_handler.isReduceRequested()

    def isMasterLive(self):
        return self.master_multicast.isMasterLived()

    '''
    Metody otwierajaca/zamykajace pewnie procesy/watki lub zadania
    oraz ustawiajace flagi, mowiace o trwaniu/zakonczeniu tychze procesow/watkow i zadan
    '''

    def finishedMap(self):
        self.worker_con_handler.registerMapEnd()
        self.master_client.finishedMap()
        self.closeMapDataFile()

    def finishedReduce(self):
        self.worker_con_handler.registerReduceEnd()
        self.master_client.finishedReduce()

    '''
    Metody sluzace do wywolywania roznych procesow/watkow 
    '''
    def runMapPairManager(self):

        self.map_pair_manager = MapPairManager(self)
        while not self.map_proc.isRun():
            pass#czekaj az sie uruchomi proces

        self.map_pair_manager.run(self.map_proc.getProcRef())

    def runReducePairManager(self):
        self.reduce_pair_manager = ReducePairManager(self, self.dataToReducePath(), self.reducePath())
        self.reduce_pair_manager.run(None)


    def runMapProcess(self):
        try:
            self.map_proc = MapProcess(self.mapPath(), self.mapDataFilePath(), None)
            self.map_proc.openInputFile()
            self.map_proc.runProcess()
        except Exception as e:
            self.fatalError("Cannot run map process !")
            raise e

    def startServer(self):
        self.worker_server.startServing()
        self.debugLog("Start Worker server Thread")

    '''
    Metody sluzace do wypisywania logow
    '''
    def logging_init(self):
        logging.basicConfig(level=self.config_reader.getWorkerLogLevel())

    def infoLog(self, msg):
        self.logger.info(msg)

    def warningLog(self,msg):
        self.logger.warning(msg)

    def debugLog(self, msg):
        self.logger.debug(msg)

    def fatalError(self, msg):
        self.logger.critical(msg)

    """
    Metoda uruchamiajaca Workera od stanu poczatkowego
    Moze sluzyc do testowania, po recznym ustawieniu stanu, na tescie ktorego nam zalezy
    oraz wywolaniu z parametrem "testing" == True
    """
    def runWorker(self, testing=False):
        if not testing:
            try:
                self.state.handleState()
            except State.ChangingStateException as e:
                if isinstance(e, MainStates.EndOfWork):
                    raise MainStates.EndOfWork()
                self.state = MainStates.state_map[str(e)](self)

        else:
            try:
                self.state.handleState()
            except State.ChangingStateException as e:
                self.closeAllResources()
                raise EndOfTestException(e)


    '''
    Metody tworzace komponenty Workera
    '''

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
        self.master_client.openConnection(self.master_ip, self.config_reader.getMasterPort())
        self.debugLog("Client Connection with Master opened")

    def createWorkerServer(self):
        self.worker_server.createThreadedServer()

    """
    Rozne 
    """
    def registerResultIntoMaster(self, key, value):
        self.master_client.registerResult(key, value)

    def registerWorker(self):
        self.master_client.registerWorker(self.config_reader.getWorkerIp(), self.config_reader.getWorkerServerPort())
        self.debugLog("Registered on Master ")

    def saveMasterIp(self, master_ip):
        self.master_ip = master_ip

    def closeMapDataFile(self):
        self.worker_con_handler.closeMapDataFile()

    def closeAllResources(self):
        self.master_multicast.close()

    def connectWithNewMaster(self):
        raise NotImplemented()

    def resetWorker(self):
        self.worker_con_handler.resetAll()
        self.infoLog("Worker connection handler reset")

        self.master_client.closeConnection()
        self.infoLog("Master client reset - connection to master closed")

        self.master_multicast.reset()
        self.infoLog("Master multicast reset")

        self.worker_server.closeServer()
        self.infoLog("Worker server closed")

        self.worker_server = WorkerServer(my_ip=self.config_reader.getWorkerIp(),
                                          my_port=self.config_reader.getWorkerServerPort(),
                                          handler=self.worker_con_handler)

        self.master_ip = None
        self.map_proc = None
        self.map_pair_manager = None
        self.reduce_pair_manager = None



