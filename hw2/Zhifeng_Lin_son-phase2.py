from Zhifeng_Lin_apriori import apriori
from Zhifeng_Lin_apriori import SUPPORT_RATIO
import MapReduce
import sys
import math

mr = MapReduce.MapReduce()

def parseCandidatesInput(rawData):
    ret = []
    for line in rawData:
        line = (line.strip())[1:-1]
        line = line.replace(' ', '')
        ret.append(map(lambda x: int(x), line.split(',')))
    return ret

def mapper(baskets):
    # parse the output from phase1 into a candidate list
    phase1OpFileName = sys.argv[2]
    phase1OpData = open(phase1OpFileName, 'r')
    candidates = parseCandidatesInput(phase1OpData)
    for candidate in candidates:
        local_cnt = 0
        # capture the candidate cnt within the chunk
        for basket in baskets:
            if set(candidate).issubset(basket):
                local_cnt += 1
        # need to send the number of baskets within the chunk to the reducer as well        
        mr.emit_intermediate(tuple(candidate), (local_cnt, len(baskets)))

def reducer(key, cnts):
    #global_baskets_cnt = reduce(lambda x,y: x[1]+y[1], cnts) 
    #global_itemSets_cnt = reduce(lambda x,y: x[0]+y[0], cnts) 
    global_baskets_cnt = 0
    global_itemSets_cnt = 0
    for cnt in cnts:
        global_itemSets_cnt += cnt[0]
        global_baskets_cnt += cnt[1]
    # only print out the global freq. itemsets and their cnt
    if global_itemSets_cnt >= int(math.ceil(global_baskets_cnt * SUPPORT_RATIO)): 
        mr.emit((key, global_itemSets_cnt))

if __name__ == '__main__':
    # read the baskets file into a list separated by newline
    inputdata = open(sys.argv[1], 'r')
    mr.execute(inputdata, mapper, reducer)

