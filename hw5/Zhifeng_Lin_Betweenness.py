import json 
import sys

class Node:
    def __init__(self, name):
        self.id = name
        # adjacency node list
        self.adjNs = []
        self.p = 1
        self.p_prime = 1
        self.parents = []
        self.children = []

    def __str__(self):
        return self.id

def createGraph(inputdata):
    # nodes is a list of Node objects
    nodes = []
    # edges is simply a nodePair tuple -> edgeFraction dictionary
    edges = {}
    # use a dictionary to avoid duplicate
    nodeDict = {}
    for line in inputdata:
        nodePair = tuple(sorted(json.loads(line)))
        name0 = nodePair[0]
        name1 = nodePair[1]
        if name0 not in nodeDict.keys():
            node0 = Node(name0)
            nodeDict[name0] = node0
            nodes.append(node0)
        if name1 not in nodeDict.keys():
            node1 = Node(name1)
            nodeDict[name1] = node1
            nodes.append(node1)
        nodeDict[name0].adjNs.append(nodeDict[name1])
        nodeDict[name1].adjNs.append(nodeDict[name0])
        edges[nodePair] = 0.0
    return nodes, edges

# create a DAG while running bfs
# meanwhile, calculate the p value for each node top-down
def bfsToDAG(start, nodes):
    # reest the values for each node
    for node in nodes: 
        node.parents = []
        node.children = []
        node.p = 1
        node.p_prime = 1
    
    # keep track of what have been visited    
    visited = set()
    curLevel = [start]
    # traverse level by level 
    while curLevel:
        nextLevel = []
        for node in curLevel:
            visited.add(node)
            # update the p value from the node's parents
            if node.parents:
                node.p = sum([parent.p for parent in node.parents])
            for child in node.adjNs:
                # only consider child that's not in the current level and
                # has not been visited 
                if child not in curLevel and child not in visited:
                    # set up the parent pointer 
                    child.parents.append(node)
                    # set up the children pointer
                    node.children.append(child)
                    # avoid duplicate in the nextLevel
                    if child not in nextLevel:
                        nextLevel.append(child)
        # go to the next level                
        curLevel = nextLevel

def calEdgeFraction(rP, child):
    # denominator is the sum of the p values of all of the child's parents
    denominator = float(sum([parent.p for parent in child.parents]))
    numerator = float(rP) * float(child.p_prime)
    return (numerator / denominator)

# post_order traversal
def bottomUpTraversal(node, edges, visited):
    for child in node.children:
        bottomUpTraversal(child, edges, visited)
    # make sure we don't double count a visited node!!!
    if node in visited:
        return
    else:
        visited.add(node)
    # calculate each node's p_prime value and update the edge fraction on the fly    
    for child in node.children:
        edgeFraction = calEdgeFraction(node.p, child)
        nodePair = tuple(sorted([node.id, child.id]))
        edges[nodePair] += edgeFraction
        node.p_prime += edgeFraction
       
def finalUpdateEdges(edges):      
    jenc = json.JSONEncoder()
    for nodePair in sorted(edges.keys()):
        edges[nodePair] /= 2.0
        print str(jenc.encode(nodePair)) + ':', edges[nodePair]

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'incorrect number of arguments'
        sys.exit(-1)
    inputdata = open(sys.argv[1])
    nodes, edges = createGraph(inputdata)
    # run bfs starting on each node
    for node in nodes: 
        bfsToDAG(node, nodes)
        # calculate edge fraction in a bottom-up fashion
        visited = set()
        bottomUpTraversal(node, edges, visited)
    # final update (divided by 2) and print output    
    finalUpdateEdges(edges)

