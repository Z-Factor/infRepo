import sys
#import re
import math
import json
from itertools import combinations as iter_combination


SUPPORT_RATIO = 0.3

def parseInput(rawData):
    baskets = []
    for line in rawData:
        # use json.loads to parse input
        baskets += json.loads(line)

# My own way of parsing the input data using the re module
# Turns out json.loads does the job for me
#
#        line = (line.strip())[1:-1]
#        line = line.replace(' ','')
#        basketsStr = re.findall('\[([0-9,]+)\]',line)
#        for bStr in basketsStr:
#            baskets.append(sorted(map(lambda x: int(x), bStr.split(','))))
    return baskets


def findCandidatesAndItemSets(k, baskets, prevItemSets, threshold):
    # for 1-itemset, every item is considered as candidate
    candidates = {}
    if k == 1:
        for basket in baskets:
            for c in iter_combination(basket, k):
                if c not in candidates:
                    candidates[c] = 1
                else:
                    candidates[c] += 1
    else:
        # construct candidates using a pair from the pool of k-1 freq. itemsets
        for c in iter_combination(prevItemSets, 2):
            # reduce it into a single itemset
            c = tuple(sorted(reduce(lambda x, y: set(x+y), c)))
            # if size of c is greater than k, don't consider it
            if len(c) != k:
                continue
            cList = list(c)
            invalidCandidate = False
            for c_subset in iter_combination(cList, k-1):
                # if any of the immediate subset is not a freq. itemset, drop it
                if c_subset not in prevItemSets:
                    invalidCandidate = True
                    break

            if invalidCandidate:
                continue
            
            # don't double count it if it's already considered
            if c in candidates:
                continue
            
            # do the counting and store it into a dictionary
            for basket in baskets:
                if set(cList).issubset(basket):
                    if c not in candidates:
                        candidates[c] = 1
                    else:
                        candidates[c] += 1
    
    # return the candiates and the freq. itemsets as a tuple                    
    return (sorted(candidates.keys()),
            sorted(map(lambda (key,value): key, filter(lambda (key,value): value >= threshold, candidates.items()))))

def apriori(baskets, prRst):
    threshold = int(math.ceil(len(baskets) * SUPPORT_RATIO))
    k = 1
    prevItemSets = []
    k_itemSets = [-1]
    candidates = [-1]
    ret = []
    # only continue if both candidates and k_itemsets are not empty
    while candidates and k_itemSets:
        candidates, k_itemSets = findCandidatesAndItemSets(k, baskets, prevItemSets, threshold)
        prevItemSets = k_itemSets
        k_itemSetsList = map(lambda t: list(t), k_itemSets)
        if prRst:
            print 'C'+str(k)+':', map(lambda t: list(t),candidates)
            print 'L'+str(k)+':', k_itemSetsList
        k += 1
        # only append it to ret if it's not empty
        if k_itemSetsList:
            ret.append(k_itemSetsList)
    
    return ret

if __name__ == '__main__':
    inputdata = open(sys.argv[1], 'r')
    apriori(parseInput(inputdata), True)
