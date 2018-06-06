from src.MapReduce.Worker.States import MainStates
from src.MapReduce.Worker.Worker import Worker, EndOfTestException

TESTING = False
TESTING_STATE = "WAIT_FOR_WORK"


def main():

    worker = Worker()
    worker.readConfiguration()
    try:
        if TESTING:
            print("Testing")
            worker.state = MainStates.state_map[TESTING_STATE](worker)
        while 1:
            worker.runWorker(testing=TESTING)
    except EndOfTestException as e:
        print(str(e))


main()
