import MapReduce
import sys
import re
from itertools import combinations as iter_combination


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key, value)
    mr.emit_intermediate(value, key)

def reducer(key, list_of_values):
    fd_list = sorted(list_of_values)
    for pair in iter_combination(fd_list, 2):
        mr.emit((pair[0], pair[1], key))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
