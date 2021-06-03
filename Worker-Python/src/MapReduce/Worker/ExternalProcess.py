
import subprocess
import threading


class ProcessStillRunException(Exception):
    pass

class BadExitecProcessException(Exception):
    def __init__(self,msg, proc_ret_code):
        Exception.__init__(self, msg)
        self.proc_ret_code = proc_ret_code

class ExternalProcess():
    def __init__(self,script_path , input_path, output_path):
        self.script_path = script_path
        self.input_path = input_path
        self.output_path = output_path
        self.input_f = None
        self.output_f = None
        self.is_run = False
        self.lock = threading.Lock()
        self.PROCESS_OK = 0
        self.proc = None

    def __call__(self, *args, **kwargs):
        try:
            self.proc = subprocess.Popen(args=["python3", self.script_path],
                                   stdin=self.input_f, stdout=self.output_path,  universal_newlines=True)
        except Exception as e:
            print(e)
            print("po procesie map")

        self.closeFiles()

        self.lock.acquire()  # SK
        self.is_run = True
        self.lock.release()  # End SK


    def runProcess(self):
        if self.is_run:
            raise ProcessStillRunException("ExternalProcess: Try to run same process twice")
        thread = threading.Thread(group=None, target=self, name='Map caller Thread')
        # w osobnym wątku, zeby nie czekac na zakonczenie procesu
        thread.start()

    def openInputFile(self):
        self.input_f = open(file=self.input_path, mode="r", encoding='utf8')

    def openOutputFile(self):
        self.output_f = open(file=self.output_path, mode="w", encoding='utf8')

    def closeFiles(self):
        if self.input_f is not None:
            self.input_f.close()
        if self.output_f is not None:
            self.output_f.close()

    def isRun(self):
        self.lock.acquire()
        ret = self.is_run
        self.lock.release()
        return ret

    def getProcRef(self):
        return self.proc

class MapProcess(ExternalProcess):
    def __init__(self,map_path , data_path, output_path):
        ExternalProcess.__init__(self, map_path, data_path, subprocess.PIPE)


class ReduceProcess(ExternalProcess):
    def __init__(self,reduce_path , data_path, output_path):
        ExternalProcess.__init__(self, reduce_path, data_path, subprocess.PIPE)

