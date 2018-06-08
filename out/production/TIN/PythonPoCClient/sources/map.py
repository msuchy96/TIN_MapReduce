import sys
def read_stdin():
    readline = sys.stdin.readline()
    while readline:
        yield readline
        readline = sys.stdin.readline()

for line in read_stdin():
    line = line.split()
    for word in line:
      print(word + '=>' + str(1))