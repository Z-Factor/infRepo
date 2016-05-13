import json 
import MapReduce
import sys

mr = MapReduce.MapReduce()

def parseMovies(rawData):
    users = {}
    for line in rawData:
        user_movies = json.loads(line)
        users[user_movies[0]] = user_movies[1]
    return users

# calculate the jaccard similarity ==> |intersection| / |union|
def calJaccard(setA, setB):
    return float(len(setA & setB)) / float(len(setA | setB))

# sort by the jaccard similarity value when considering candidates
def byJaccard(candidate):
    return (-1*candidate[1], candidate[0])

def mapper(record):
    userA = record[0]
    userB = record[1]
    # read the first file for user-movies information
    moviesData = open(sys.argv[1], 'r')
    userInfos = parseMovies(moviesData)
    moviesA = set(userInfos[userA])
    moviesB = set(userInfos[userB])
    jSim = calJaccard(moviesA, moviesB)
    # movies that is watched by B but not A
    recommA = moviesB - moviesA
    # movies that is watched by A but not B
    recommB = moviesA - moviesB
    # emit key-value tuple for both users in the pair
    mr.emit_intermediate(userA, (userB, jSim, list(recommA)))
    mr.emit_intermediate(userB, (userA, jSim, list(recommB)))


def reducer(user, values):
    candidates = sorted(values, key=byJaccard)
    moviesViewCnt = {}
    top5Jacard = 0
    curJacard = -1
    for candidate in candidates:
        # only consider the top five
        if top5Jacard < 5 or curJacard != candidate[1]:
            curJacard = candidate[1]
            top5Jacard += 1
        if top5Jacard > 5:
            break
        movies = candidate[2]
        for movie in movies:
            if movie not in moviesViewCnt.keys():
                moviesViewCnt[movie] = 0
            moviesViewCnt[movie] += 1
    # only recommend movies that are watched by at least three other similar users        
    moviesRecomm = map(lambda (key, value): key, filter(lambda (movie, cnt): cnt >= 3, moviesViewCnt.items()))
    if moviesRecomm:
        mr.emit((user, sorted(moviesRecomm)))

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'Incorrect use of program'
    else:
        inputdata = open(sys.argv[2])
        mr.execute(inputdata, mapper, reducer)

