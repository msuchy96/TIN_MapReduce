from src.MapReduce.Worker.States.State import InterruptableState, OneShotState, ChangingStateException



class NotImplemented(Exception):
    pass


class WaitingForMasterState(InterruptableState):
    def __init__(self, worker_ref):
        InterruptableState.__init__(self, worker_ref)
        #uruchom watek sluchajacy na multicascie mastera

    def handleState(self):
        print("Begin")
        (msg, sender_addr) = self.worker_ref.master_multicast.receive()
        print("After recv")
        if msg == self.MASTER_JOIN_COM:
            #zapisz adres mastera
            self.worker_ref.saveMasterIp(sender_addr[0])
            print(sender_addr)
            raise ChangingStateException("SET_CLIENT_THRIFT_WITH_MASTER")
        else:
            print ("Message from master multicast: " + msg)
            print ("Adress master: " )


class SetClientThriftWithMasterState(OneShotState):
    def __init__(self, worker_ref):
        OneShotState.__init__(self, worker_ref)


    def handleState(self):
        #utworz polaczenie z masterem
        self.worker_ref.createClientConnectionWithMaster()
        self.worker_ref.createWorkerServer()
        self.registerWorker()
        raise ChangingStateException("WAIT_AS_THRIFT_SERVER_FOR_MASTER")
#        raise NotImplemented("Set Client Thrift with master")



class WaitAsThriftServerForMasterState(InterruptableState):
    def __init__(self, worker_ref):
        super.__init__(self, worker_ref)

    def handleState(self):
        raise ChangingStateException("MASTER_CONNECTED")




class MasterConnectedState(OneShotState):
    def __init__(self, worker_ref):
        super.__init__(self, worker_ref)

    def handleState(self):
        raise ChangingStateException("WAIT_FOR_WORK")




class WaitForWorkState(InterruptableState):
    def __init__(self, worker_ref):
        InterruptableState.__init__(self, worker_ref)

    def handleState(self):
        while 1:
            if not self.isMasterLive():
                raise NotImplemented("Wait for work")
            map_request = self.worker_ref.isMapRequested()
            if map_request :
                raise ChangingStateException("CATCH_DATA_FROM_MASTER")
        raise NotImplemented("Wait for work")

class CatchDataFromMaster(OneShotState):
    def __init__(self, worker_ref):
        OneShotState.__init__(self, worker_ref)

    def handleState(self):
        raise ChangingStateException("MAP_STEP")


class MapStep(InterruptableState):
    def __init__(self, worker_ref):
        InterruptableState.__init__(self, worker_ref)

    def handleState(self):
        pass



state_map = {
    "WAIT_FOR_MASTER": WaitingForMasterState,
    "SET_CLIENT_THRIFT_WITH_MASTER": SetClientThriftWithMasterState,
    "WAIT_AS_THRIFT_SERVER_FOR_MASTER": WaitAsThriftServerForMasterState,
    "MASTER_CONNECTED": MasterConnectedState,
    "WAIT_FOR_WORK": WaitForWorkState,
    "CATCH_DATA_FROM_MASTER": CatchDataFromMaster,
    "MAP_STEP": MapStep,


}
