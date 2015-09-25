import gzip

def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield eval(l)
