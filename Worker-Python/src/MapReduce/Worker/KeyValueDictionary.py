from src.MapReduce.Worker import Utilities


class PairDictionary():
    def __init__(self, pairs_file_path):
        self.dictionary = {}
        self.pairs_file_path = pairs_file_path
        self.pairs_file = None

    def addPair(self, key, value, quantity):
        bucket = []
        for i in range(int(quantity)):
            bucket.append(value)
        #act_bucket = self.dictionary[Utilities.Hash.getHash(key)]
        #bucket = act_bucket + bucket
        try:
            self.dictionary[str(key)] += bucket
        except KeyError as e:
            self.dictionary[str(key)] = []
            self.dictionary[str(key)] += bucket

    def openFile(self):
        self.pairs_file = open(self.pairs_file_path, mode='r')

    def loadPairsToDictionary(self):
        #self.pairs_file = open("elo", "r")
        for line in self.pairs_file:
            key, value = Utilities.PairParser.parseStringToKeyValue(str(line))
            self.addPair(key, value, 1)

    def getValuesArray(self, key):
        return self.dictionary[key]


