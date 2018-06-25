import socket
import struct



import copy

from src.MapReduce.Worker import Utilities
import ipaddress

from threading import Lock, Thread

MCAST_GROUP = "224.0.0.123"
MCAST_PORT = 9090
BUF_SIZE = 4096

class NoneFirstMasterException(Exception):
    pass

class TwiceThreadRunningException(Exception):
    pass

class MasterStillLiveException(Exception):
    pass


class MulticastListener:
    def __init__(self, mcast_group, my_port):
        self.mcast_group = mcast_group
        self.port = my_port

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.buffer = []
        self.bindPorts()

    def bindPorts(self):
        self.socket.bind( ('', self.port ) )
        ip = socket.inet_aton(self.mcast_group)#.encode('utf8')
        mreq = struct.pack("4sL", ip, socket.INADDR_ANY)
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    def receive(self):
        msg, sender = self.socket.recvfrom(BUF_SIZE)
        self.buffer.append(msg)
        return msg, sender #ostatni element, czyli ten co dodalismy


    def close(self):
        self.socket.close()


class MasterMulticastListener(MulticastListener):
    def __init__(self, mcast_group, my_port):
        MulticastListener.__init__(self, mcast_group, my_port)
        self.master_lived = None
        self.new_master_adress = None
        self.mutex = Lock()
        self.MASTER_JOIN_COM = "HELLOWORKERS"
        self.NEW_MASTER_COM = "IAMNEWMASTER"
        self.listening_thread = None
        self.stop_thread = False
    '''
    Po to, aby watek nasluchujacy mogl wywolac receive
    '''
    def __call__(self, *args, **kwargs):
        while not 1:
            self.mutex.acquire()
            must_close = self.stop_thread
            self.mutex.release()
            if must_close:
                break

            self.receive()

    '''
    Uruchamia watek sluchajacy czy nie nadejdzie komunikat ze master zginal 
    '''
    def runListeningThread(self):
        if self.listening_thread is not None:
            raise TwiceThreadRunningException("MasterMulticastListener: Try to run next listenign thread")
        self.listening_thread = Thread(group=None, target=self, name='Multicast Listener Thread')
        self.listening_thread.start()


    '''
    Slucha w oczekiwaniu na wiadomosc UDP
    Ponadto zapisuje, jesli nadejdzie wiadomosc, ze master umarl, i nalezy go zastapic(jezeli oczywiscie zginal)
    '''
    def receive(self):
        msg, sender = MulticastListener.receive(self)
        #print(msg)
        if msg == self.NEW_MASTER_COM and self.master_lived is not None and self.master_lived is True:

            #stary master padl - zapisz adres nowego mastera
            self.mutex.acquire()  # SK
            self.new_master_adress = sender[0]
            self.master_lived = False
            self.mutex.release()#END SK

        return msg, sender

    def getAdressOfNewMaster(self):
        self.mutex.acquire()#SK
        ret = self.new_master_adress
        self.mutex.release()#SK

        if ret is None:
            raise MasterStillLiveException("MasterMulticastListener "
                                           "- try to load new master adress while none master "
                                           "connect or master still lie")
        return ret

    def firstConnectionActive(self):
        self.master_lived = True

    def isMasterLived(self):
        if self.master_lived is None:
            raise NoneFirstMasterException("MasterMulticastListener: none connection from first master")
        self.mutex.acquire() #SK
        ret = self.master_lived
        self.mutex.release() #END SK
        return ret

    def resetFlags(self):
        self.master_lived = None
        self.new_master_adress = None
        self.listening_thread = None
        self.stop_thread = False

    def reset(self):
        #zamknij watek
        self.mutex.acquire()
        self.stop_thread = True
        self.mutex.release()
        self.listening_thread.join()
        self.resetFlags()

