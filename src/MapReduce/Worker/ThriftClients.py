
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from ..MasterServices import MapReduceMaster

class ClientThriftConnection:
    def __init__(self):
        self.thrift_client = None
        self.transport = None#TSocket.TSocket(server_ip, server_port)
        self.transport = None#TTransport.TBufferedTransport(self.transport)
        self.protocol = None#TBinaryProtocol.TBinaryProtocol(self.transport)

    def createClient(self):
        pass

    def openConnection(self, server_ip, server_port):
        self.transport = TSocket.TSocket(server_ip, server_port)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.transport.open()
        self.createClient()

    def cloneConnection(self):
        self.transport.close()


'''

Klasa sluzaca do obslugi polaczenia z masterem, w roli klienta.
Tutaj mozemy wywolywac zdalnie metody na masterze.

'''


class MasterServiceClientConnection(ClientThriftConnection):
#    def __init__(self, server_ip, server_port):
#        super.__init__(self, server_port, server_ip)

#    def __init__(self):
#        super.__init__(self)

    def createClient(self):
        self.thrift_client = MapReduceMaster.Client(self.protocol)

    def registerWorker(self, worker_server_ip, worker_server_port):
        self.thrift_client.RegisterWorker(worker_server_ip, worker_server_port)

    def reconnect(self):
        self.thrift_client.Reconnect()

    def finishedMap(self):
        self.thrift_client.FinishedMap()

    def finishedReduce(self):
        self.thrift_client.FinishedReduce()

    def registerResult(self, key, value):
        self.thrift_client.RegisterResult(key, value)



