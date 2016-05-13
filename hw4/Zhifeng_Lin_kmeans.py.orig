import json 
import sys
import math
from itertools import combinations as iter_combination

def createDataPoints(inputdata):
    points = []
    for line in inputdata:
        # turn list into tuple in order to store in a set
        point = tuple(json.loads(line))
        points.append(point)
    return points

def calDistance(p1, p2):
    distance = 0.0
    for dim in xrange(len(p1)):
        distance += (p1[dim] - p2[dim]) * (p1[dim] - p2[dim])
    distance = math.sqrt(float(distance))
    return distance

def findMinDistanceFromCentroids(centroids, point):
    distance = sys.maxint
    for c in centroids:
        dis = calDistance(c, point)
        if dis < distance:
            distance = dis
    return distance

def createCentroids(data_points, k):
    centroids = []
    # first point is always a centroid
    centroids.append(data_points[0])
    k -= 1
    pointsSet = set(data_points[1:])
    while k > 0:
        k -= 1
        maxDis = -1
        maxPoint = None
        for p in pointsSet:
            dis = findMinDistanceFromCentroids(centroids, p)
            # find the farthest point from the already-found centroids
            if dis > maxDis:
                maxDis = dis
                maxPoint = p
        centroids.append(maxPoint)
        pointsSet.remove(maxPoint)
    return centroids

def assignClusters(centroids, points):
    centroidsMap = {}
    for point in points:
        minDis = sys.maxint
        minCentroid = None
        for c in centroids:
            dis = calDistance(c, point)
            if dis < minDis:
                minDis = dis
                minCentroid = c
        if minCentroid not in centroidsMap:
            centroidsMap[minCentroid] = []
        # assign the point to the closest centroid    
        centroidsMap[minCentroid].append(point)
    
    newCentroidsMap = {}
    for c in centroidsMap.keys():
        # recompute the centroid for each cluster
        newCentroid = reduce(lambda p1, p2: [sum(x) for x in zip(p1, p2)], centroidsMap[c])  
        num = float(len(centroidsMap[c]))
        newCentroid = map(lambda x: float(x/num), newCentroid)
        newCentroidsMap[tuple(newCentroid)] = sorted(centroidsMap[c])
    return newCentroidsMap

def kmeans(data_points, k):
    prevCentroids = sorted(createCentroids(data_points, k))
    centroidsMap = assignClusters(prevCentroids, data_points)
    curCentroids = sorted(centroidsMap.keys())
    # perform point assignment until centroids become stable
    while curCentroids != prevCentroids:
        prevCentroids = curCentroids
        centroidsMap = assignClusters(prevCentroids, data_points)
        curCentroids = sorted(centroidsMap.keys())
    
    sumDiameter = 0
    for c in centroidsMap.keys():
        maxDiameter = 0 
        for pair in iter_combination(centroidsMap[c], 2):
            dis = calDistance(pair[0], pair[1])
            if dis > maxDiameter:
                maxDiameter = dis
        # add up all the diameters for the cluster
        sumDiameter += maxDiameter
    return centroidsMap.values(), float(sumDiameter/k)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'incorrect number of arguments'
        exit(-1)
    inputdata = open(sys.argv[1])
    points = createDataPoints(inputdata)
    k = int(sys.argv[2])
    if k > len(points):
        print 'k cannot be greater than the number of points'
        exit(-1)
    clusters, cohesion = kmeans(points, k)
    for cluster in clusters:
        print cluster
    print cohesion

