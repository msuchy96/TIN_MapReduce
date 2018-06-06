


class ChangingStateException(Exception):
    pass


class State:
    def __init__(self, worker_ref):
        self.worker_ref = worker_ref
        self.MASTER_JOIN_COM = "HELLOIMMASTER"

    def isMasterLive(self):
        return self.worker_ref.isMasterLive()

    def handleState(self):
        raise NotImplementedError


class InterruptableState(State):
    def __init__(self, worker_ref):
        State.__init__(self, worker_ref)

    def handleState(self):
        State.handleState()


class OneShotState(State):
    def __init__(self, worker_ref):
        super.__init__(self, worker_ref)

    def handleState(self):
        State.handleState()

