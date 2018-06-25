import json

import logging


class ConfigReader:

    CONFIG_FILE = 'config.json'
    LOG_LEVELS = {
        "DEBUG":logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR" : logging.ERROR,
        "CRITICAL": logging.CRITICAL

    }
    def __init__(self):
        self.json_dictionary = {}
        self.readConfiguration(ConfigReader.CONFIG_FILE)

    def readConfiguration(self, config_file_path):
        with open('config.json') as f:
            self.json_dictionary = json.load(f)

    '''
    Worker config
    '''
    def getWorkerIp(self):
        general = self.json_dictionary['GeneralConfiguration']
        return general['Ip']

    def getWorkerServerPort(self):
        general = self.json_dictionary['GeneralConfiguration']
        return general['ServerPort']

    def getWorkerMulticastPort(self):
        general = self.json_dictionary['GeneralConfiguration']
        return general['MulticastPort']

    def getWorkerLogLevel(self):
        general = self.json_dictionary['GeneralConfiguration']
        log_str = general['LogLevel']
        return ConfigReader.LOG_LEVELS[log_str]


    '''MasterConfig'''
    def getMasterMulticastIp(self):
        master = self.json_dictionary['MasterConfig']
        return master['MasterMulticastGroup']

    def getMasterPort(self):
        master = self.json_dictionary['MasterConfig']
        return master['MasterServerPort']

    '''
    Filesystem config
    '''
    def getRootDirectory(self):
        filesystem =  self.json_dictionary['FilesystemConfig']
        return filesystem['FilesystemRoot']




