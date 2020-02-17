import time
from pyspark import SparkContext
import sys
import random
from collections import defaultdict
import itertools

def findPossibleCandidates(band):
    hashes = defaultdict(list)
    for business_id in range(0, len(band)):
        hashes[hash(band[business_id])].append(diff_bus_id[business_id])

    result=possibleCombinations(hashes)
    return result

def possibleCombinations(hashes):
    poss_cand_pairs = []
    for bus_list in hashes.values():
        if len(bus_list) >= 2:
            for each in itertools.combinations(bus_list,2):
                poss_cand_pairs.append(each)
    return poss_cand_pairs

def calculateHash(a,b,j,m):
    return ((a * j) + b) % m

def getSignature(row):
    signatures=[]
    for i in range(0,80):
        x1 = x_vals[i]
        y1 = y_vals[i]
        min=sys.maxsize
        for j in row[1]:
            hash = calculateHash(x1,y1,j,num_of_users)
            if hash<min:
                min=hash
        signatures.append(min)
    return signatures

def divideBands(sig_row):
    band_values=[]
    for i in range(bands):
        temp=[]
        for j in range(rows):
            temp.append(sig_row[i*rows+j])
        band_values.append((i,str(temp)))
    return band_values

def find_jaccard_pairs(pair):

    one=frozenset(matrix_dict[pair[0]])
    two=frozenset(matrix_dict[pair[1]])
    similarity = float(len(one&two)/len(one|two))
    return (pair,similarity)


start=time.time()

input_file=sys.argv[1]
case=sys.argv[2]

if(case=="jaccard"):

    sc=SparkContext('local[*]', 'darshan_dm3_task1')
    input=sc.textFile(input_file)
    header=input.first()
    data=input.filter(lambda a: a != header)
    data=data.distinct().map(lambda a: a.split(","))

    diff_users=data.map(lambda a:a[0]).distinct().collect()
    diff_bus_id=data.map(lambda a:a[1]).distinct().collect()
    diff_users.sort()
    diff_bus_id.sort()

    matrix=data.map(lambda a: (a[1],a[0])).groupByKey().map(lambda a:(a[0],list(a[1]))).sortByKey()
    index_matrix=matrix.map(lambda a:(diff_bus_id.index(a[0]),[diff_users.index(user) for user in a[1]]))

    matrix_dict=matrix.collectAsMap()

    x_vals=[random.randint(1,500) for i in range(80)]
    y_vals=[random.randint(1,100) for j in range(80)]

    rows=2
    bands=40

    num_of_users=len(diff_users)
    sig_matrix=index_matrix.map(getSignature).flatMap(divideBands).groupByKey().map(lambda a:list(a[1]))

    possible_candidates=sig_matrix.flatMap(lambda a:findPossibleCandidates(a)).distinct()


    candidates_jaccard=possible_candidates.map(find_jaccard_pairs).filter(lambda a:a[1]>=0.5)
    candidates_jaccard=candidates_jaccard.sortBy(lambda a:(a[0][0],a[0][1])).collectAsMap()

    s=""
    result_file=sys.argv[3]
    for similar_pair in candidates_jaccard:
        s=s+similar_pair[0]
        s=s+","+similar_pair[1]
        s=s+","+str(candidates_jaccard[similar_pair])
        s=s+"\n"
    result=open(result_file,"w")
    result.write("business_id_1, business_id_2, similarity\n")
    result.write(s)
    result.close()
else:
    result_file=sys.argv[3]
    result=open(result_file, "w")
    result.write("Not Implemented")
    result.close()
    print("Not implemented")


print("Duration:",time.time()-start)


