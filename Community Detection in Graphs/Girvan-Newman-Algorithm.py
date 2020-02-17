from pyspark import SparkContext
import time
import math
import sys
from collections import defaultdict

def getBFS(node,adj_list):
    q=[]
    bfs = {}
    seen = [False] * (len(adj_list)+1)
    q.append(node)
    bfs[node] = 0
    max_lev=0
    seen[node]=True
    while len(q)!=0:
        first=q.pop(0)
        for neighbor in adj_list[first]:
            if seen[neighbor]==False:
                seen[neighbor] = True
                q.append(neighbor)
                bfs[neighbor] = bfs[first]+1
                if(bfs[first]+1)>max_lev:
                    max_lev=bfs[first]+1

    l=[set() for i in range(max_lev+1)]
    for node in bfs:
        l[bfs[node]].add(node)
    return l

def writeBetweennessTofile(betweenness,betweenness_path):
    f=open(betweenness_path,"w")
    a=[]
    for edge in betweeness:
        list_bet = []
        e1=str(edge[0][0])
        e2=str(edge[0][1])
        list_bet.append(e1)
        list_bet.append(e2)
        list_bet.sort()
        list_bet.append(edge[1])
        a.append(list_bet)
    for edge in a:
        f.write("('"+str(edge[0])+"', '"+str(edge[1])+"'), "+str(edge[2]))
        f.write("\n")


def getBetweeness(root,bfs):
    # print(root)
    # print(bfs)
    depth=len(bfs)-1
    num_sp={}

    if depth > 0:
        for vertex in bfs[1]:
            num_sp[vertex]=1

    for l in range(2, len(bfs)):
        active_vertex = bfs[l]
        parent_candidates = bfs[l - 1]
        for vertex in active_vertex:
            parent = parent_candidates & set(adj_list[vertex])
            num = 0
            for each in parent:
                num += num_sp[each]
            num_sp[vertex]=num
    num_sp[root]=1

    bfs.reverse()
    bfs_without_root = bfs[:-1]

    edge_score = defaultdict(float)
    scores = defaultdict(float)

    i=0
    for level_nodes in bfs_without_root:
        for node in level_nodes:

            if i == 0:
                scores[node] = 1
            else:
                c = (set(adj_list[node])) & bfs[i - 1]
                for child in c:
                    scores[node] += edge_score[tuple(sorted((node,child)))]
                scores[node] += 1
            p =  (set(adj_list[node])) & bfs[i + 1]

            all_paths_len = 0
            for parent in p:
                all_paths_len+=num_sp[parent]
            for parent in p:
                e=tuple(sorted((parent,node)))
                edge_score[e] = scores[node] * (num_sp[parent])/all_paths_len

        i+=1

    return edge_score.items()

def writeAnswerTofile(answer,output):
    f=open(output,"w")
    for each in answer:
        flag=1
        for node in each:
            if(flag==1):
                f.write("'" + str(node)+"'")
                flag=0
            else:
                f.write(", '"+str(node)+"'")
        f.write("\n")
    f.close()

def traversal(adj_list,temp,vertex,seen):
    temp.append(vertex)
    seen.add(vertex)
    for neighbor in adj_list[vertex]:
        if neighbor not in seen:
            temp=traversal(adj_list,temp,neighbor,seen)

    return temp

start = time.time()
sc = SparkContext()
betweeness_out=sys.argv[2]
communities_out =sys.argv[3]
input=sys.argv[1]
data=sc.textFile(input)
edges1=data.map(lambda row: row.split(" ")).map(lambda row:(int(row[0]),[int(row[1])]))
edges2=data.map(lambda row: row.split(" ")).map(lambda row:(int(row[1]),[int(row[0])]))
node_set=edges1.union(edges2).reduceByKey(lambda x,y : x+y).sortByKey()
adj_list =node_set.collectAsMap()
m=edges2.count()
bfs=node_set.map(lambda node:getBFS(node[0],adj_list)).collect()
betweeness=node_set.flatMap(lambda node:getBetweeness(node[0],bfs[node[0]-1])).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0],x[1]/2)).sortBy(lambda x:(-x[1],x[0][0])).collect()

writeBetweennessTofile(betweeness,betweeness_out)

global_maximum = -sys.maxsize - 1
modularity = 0
final_minigraphs = []
answer=[]
flag=True
while (flag):
    adj_list[betweeness[0][0][0]].remove(betweeness[0][0][1])
    adj_list[betweeness[0][0][1]].remove(betweeness[0][0][0])

    bfs = node_set.map(lambda node: getBFS(node[0], adj_list)).collect()
    betweeness = node_set.flatMap(lambda node: getBetweeness(node[0], bfs[node[0] - 1])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1] / 2)).sortBy(lambda x: (-x[1], x[0][0])).collect()

    seen = set()
    Minigraphs = []

    for vertex in adj_list:
        if vertex not in seen:
            temp = traversal(adj_list, [], vertex, seen)
            temp.sort()
            Minigraphs.append(temp)

    original_graph=node_set.collectAsMap()
    modularity = 0
    for each in Minigraphs:
        for i in each:
            for j in each:
                if (i <= j):
                    if(j in original_graph[i]):
                        a=1
                    else:
                        a=0
                    modularity += (a - (len(original_graph[i]) * len(original_graph[j]) / (2*m)))

    modularity = modularity / (2*m)

    if (modularity > global_maximum):
        global_maximum = modularity
        final_minigraphs = Minigraphs
    if len(Minigraphs) == len(original_graph):
        flag=False

    final=[]
    for each in final_minigraphs:
        temp=[]
        for vertex in each:
            temp.append(str(vertex))
        temp.sort()
        final.append(temp)

    answer = sorted(final, key=lambda l: (len(l), l))

writeAnswerTofile(answer,communities_out)

end=time.time()
print("Duration:",end-start)



