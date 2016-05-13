import json 
import MapReduce
import sys
from itertools import combinations as iter_combination

NUM_BANDS = 5
NUM_ROWS = 4

mr = MapReduce.MapReduce()
unique_pairs = set()

def calMinhash(movies, i):
    # mapping from old row# to new row#
    mappingDict = {}
    for movie in movies:
        mappingDict[movie] = (3 * movie + i) % 100
    signature = sys.maxint
    # look at row in order and only update the signature when 
    # smaller value has been encountered
    for oldRow in sorted(mappingDict.keys()):
        s = mappingDict[oldRow]
        if s < signature:
            signature = s

    return signature
    

def mapper(record):
    user = record[0]
    movies = record[1]
    for band in xrange(NUM_BANDS):
        signatures = []
        for row in xrange(NUM_ROWS):
            # i ranges from 1 to 20
            i = band * NUM_ROWS + row + 1
            signatures.append(calMinhash(movies, i))
        # generate key-value pairs when key is the band id, value is (user, partial_signature)    
        mr.emit_intermediate(band, (user,signatures))

def reducer(band, userSignatures):
    buckets = {}
    for userSignature in userSignatures:
        signature = tuple(userSignature[1])
        if signature not in buckets:
            buckets[signature] = []
        # hash users into the same bucket if their signatures are the same within the band    
        buckets[signature].append(userSignature[0])
    for bucket in buckets.keys():
        for pair in iter_combination(buckets[bucket], 2):
            # instead of simply emitting, need to add candidate pairs to a set
            # to filter out duplicates
            unique_pairs.add(tuple(sorted(pair)))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
    jenc = json.JSONEncoder()
    # print each unique pair
    for item in unique_pairs:
        print jenc.encode(item)
