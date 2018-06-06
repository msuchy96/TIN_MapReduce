from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from ..WorkerServices import MapReduceWorker


class ServerThrift:
    def __init__(self, my_ip, my_port):
        self.handler = None
        self.processor = None
        self.server = None
        self.transport = TSocket.TServerSocket(host=my_ip, port=my_port)
        self.tfactory = TTransport.TBufferedTransportFactory()
        self.pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    def createSimpleServer(self):
        self.server = TServer.TSimpleServer(self.processor, self.transport, self.tfactory, self.pfactory)

    def createThreadedServer(self):
        self.server = TServer.TThreadedServer(self.processor, self.transport, self.tfactory, self.pfactory)

    def startServing(self):
        self.server.serve()


'''
Klasa tworzaca serwer Workera 
Po wywolaniu metody "startServing" pozwala klientom na wywolywanie metod workera
'''


class WorkerServer(ServerThrift):
    def __init__(self, my_ip, my_port, handler):
        ServerThrift.__init__(self, my_ip, my_port)
#        super(WorkerServer, self).__init__(my_ip, my_port)
        self.handler = handler
        self.processor = MapReduceWorker.Processor(self.handler)


