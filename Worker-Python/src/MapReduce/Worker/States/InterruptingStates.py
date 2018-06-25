from src.MapReduce.Worker.States.State import  OneShotState


class DeathMasterState(OneShotState):
    def __init__(self, worker_ref, return_state):
        super.__init__(self, worker_ref)
        self.return_state = return_state

    def handle(self, worker_ref):
        pass


