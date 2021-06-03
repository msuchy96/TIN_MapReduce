from src.MapReduce.Worker.States import MainStates
from src.MapReduce.Worker.Worker import Worker, EndOfTestException

TESTING = False
TESTING_STATE = "PREPARE_AND_RUN_MAP"


def main():

    worker = Worker()
    try:
        if TESTING:
            print("Testing")
            worker.state = MainStates.state_map[TESTING_STATE](worker)
        while 1:
            worker.runWorker(testing=TESTING)
    except EndOfTestException as e:
        print(str(e))
    except MainStates.EndOfWork:
        return 0


main()
