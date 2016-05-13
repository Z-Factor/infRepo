import sys
import math
from Zhifeng_Lin_kmeans import kmeans
from Zhifeng_Lin_kmeans import createDataPoints

def calRateChange(c_v, c_2v, v):
    try:
        return abs(c_v - c_2v) / float(c_v * v)
    except:
        return 0.0

def findV(data_points, theta):
    rate = sys.maxint
    k = 1
    n = len(data_points)
    needBSearch = True
    while rate > theta:
        v = k
        clusters, c_v = kmeans(data_points, k)
        k *= 2
        if k > n:
            # special case, no need to do binary search
            k = n
            needBSearch = False
        clusters, c_2v = kmeans(data_points, k)
        rate = calRateChange(c_v, c_2v, v)
    return v, needBSearch

def findKstar(data_points, theta, x, y):
    clusters, c_x = kmeans(data_points, x)
    clusters, c_y = kmeans(data_points, y)
    # base case, return whichever has the lowest avg diameter
    if (y - x) == 1:
        if c_x <= c_y: 
            return x
        else:
            return y
    # find the mid point    
    z = (x + y) / 2
    clusters, c_z = kmeans(data_points, z)
    # calculate the rate of change on the back portion of the interval
    rateBack = calRateChange(c_z, c_y, (y - z))
    if rateBack < theta:
        # if there's little change, narrow it down on [x, z]
        return findKstar(data_points, theta, x, z)
    else:
        # else narrow it down on [z, y]
        return findKstar(data_points, theta, z, y)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'incorrect number of arguments'
        exit(-1)
    inputdata = open(sys.argv[1])
    points = createDataPoints(inputdata)
    theta = float(sys.argv[2])
    v, needBSearch = findV(points, theta)
    x = v / 2
    y = v
    if needBSearch:
        kstar = findKstar(points, theta, x, y)
    else:
        kstar = len(points)
    print kstar 
