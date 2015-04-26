from math import log, ceil, e

n = float(2**23) # Number of weak hashes
_64Ko = 8*2**16
M = 8*2**20
G = 8*2**30
p = 0.001

print("Number of entries in Mo: %s" % (n/M))
print("Total storage in Go: %s" % int((n*_64Ko)/G))

m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))));
k = round(log(2.0) * m / n)
print(m)
print('m (in Mo)', int(m /(M)), 'k', k)
print('   proba of false positive', (1 - e**(-k*n/m))**k)
