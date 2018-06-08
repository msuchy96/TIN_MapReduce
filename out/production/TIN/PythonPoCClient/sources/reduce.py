import sys
def read_stdin():
    readline = sys.stdin.readline()
    while readline:
        yield readline
        readline = sys.stdin.readline()
sum=0
for line in read_stdin():
    sum+=int(line)
print(sum)