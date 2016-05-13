from Zhifeng_Lin_apriori import apriori
import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(baskets):
    k_itemSets = apriori(baskets, False)
    k = 1
    for itemSets in k_itemSets:
        for itemSet in itemSets:
            # for each k-itemset, emit (k, k-itemset)
            mr.emit_intermediate(k, itemSet)
        k += 1

def reducer(key, list_itemSets):
    itemSets = []
    for itemSet in list_itemSets:
        # don't emit duplicates
        if itemSet not in itemSets:
            mr.emit(itemSet)
            itemSets.append(itemSet)

if __name__ == '__main__':
    inputdata = open(sys.argv[1], 'r')
    mr.execute(inputdata, mapper, reducer)

