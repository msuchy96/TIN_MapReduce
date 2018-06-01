def read_stdin():
    readline = sys.stdin.readline()
    while readline:
        yield readline
        readline = sys.stdin.readline()

counter = 0
for pair in read_stdin():
    pair = pair.split("=>")
    counter += pair[1]
print(counter);