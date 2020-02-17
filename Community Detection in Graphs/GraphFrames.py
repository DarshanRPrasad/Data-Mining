from pyspark import SparkContext
import sys
from pyspark.sql import SQLContext
from graphframes import *
import time

def writeTofile(result,output):
    f=open(output,"w")
    for community in result:
        f.write(str(community)[1:-1])
        f.write("\n")
    f.close()

start=time.time()
input=sys.argv[1]
output=sys.argv[2]
sc = SparkContext()

data = sc.textFile(input)
data=data.map(lambda a:a.split(" "))

v1=data.map(lambda a:(a[0],a[0]))
v2=data.map(lambda a:(a[1],a[1]))
all_vertices=v1.union(v2).distinct().collect()

e1=data.map(lambda a:(a[0],a[1]))
e2=data.map(lambda a:(a[1],a[0]))
all_edges=e1.union(e2).collect()

sqlContext = SQLContext(sc)
e_dataframe= sqlContext.createDataFrame(all_edges, ["src", "dst"])
v_dataframe = sqlContext.createDataFrame(all_vertices, ["id", "name"])

g = GraphFrame(v_dataframe, e_dataframe)
answer = g.labelPropagation(maxIter=5)
answer=answer.rdd.map(lambda x:(x.label,[x.id])).reduceByKey(lambda x,y:x+y).map(lambda x:sorted(x[1])).sortBy(lambda x:(len(x),x)).collect()

writeTofile(answer,output)

end=time.time()
print("Duration:",end-start)