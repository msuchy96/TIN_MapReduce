

"""
Bierze pary klucz-wartosc z pliku outputu map(),
oblicza komu ma wyslac
i wysyla do odpowiedniego workera
"""

class PairManager():
    def __init__(self):
        self.is_running = False
        self.add_pairs_sent = False

    def runPairManager(self):
        pass

    def isPairsSent(self):
        return self.add_pairs_sent


