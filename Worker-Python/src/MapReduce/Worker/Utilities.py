import re
import struct
import socket


def ip2int(addr):
    return struct.unpack("!I", socket.inet_aton(addr))[0]


class Hash():
    def __init__(self):
        pass

    @staticmethod
    def getHash(key, modulo=None):
        hash = 5381
        for char in str(key).encode('utf8'):
            hash = ((hash << 5 ) + hash) + char
        if modulo is None:
            return int(hash)

        return Hash.getModulo(int(hash), modulo)

    @staticmethod
    def getModulo(divided, modulo):
        return (divided % modulo + modulo) % modulo

class PairParser():

    """
    Zakladamy, ze w stringu jest tylko jedna para klucz=>wartosc
    I ze sa one odzielone znakami =>
    Zwraca tuple (klucz, wartosc)
    """
    @staticmethod
    def parseStringToKeyValue( pair_str):
        PATTERN = r'(.*)=>(.*)'
        match_obj = re.match(PATTERN, pair_str)
        return match_obj.group(1), match_obj.group(2)

    """
    Sklada z klucza i wartosci stringa postaci : key=>value
    """
    @staticmethod
    def parseKeyValueToString(key, value):
        return str(key) + "=>" + str(value)

    """
    Sklada elementy tablicy danej jako argument w jednego stringa
    Kolejne elementy tablicy beda umieszczone w nowych liniach
    """
    @staticmethod
    def getLinedStringFromArray(str_array):
        ret_str = ''
        for elem in str_array:
            ret_str = ret_str + str(elem) + '\n'
        return ret_str


