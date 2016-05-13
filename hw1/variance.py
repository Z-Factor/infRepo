import MapReduce
import sys
import re
from itertools import combinations as iter_combination


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    key = '' 
    value = (len(record), sum(record), sum(map(lambda x: x*x, record)))
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    sum1 = 0.0
    sum2 = 0.0
    numInt = 0.0
    for numItem, sum, sqSum in list_of_values:
        numInt += numItem
        sum1 += sum
        sum2 += sqSum
    variance = sum2/numInt - (sum1/numInt) * (sum1/numInt)
    mr.emit(variance)
# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
