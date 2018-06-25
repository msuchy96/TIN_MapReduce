
import struct
import socket


def ip2int(addr):
    return struct.unpack("!I", socket.inet_aton(addr))[0]

