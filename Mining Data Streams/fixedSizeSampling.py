import sys
import json
from statistics import median
import random
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
import binascii

def getAValues(max):
    a_vals = [random.randint(500,1000) for i in range (max)]
    return a_vals

def countZeroes(bstring):
    reversed = bstring[::-1]
    count = 0
    for i in range(0, len(reversed)):
        if reversed[i] == '0':
            count += 1
        else:
            break
    return count

def getBValues(max):
    b_vals = [random.randint(120,620) for i in range (max)]
    return b_vals

def getTailZeroes(num,i):
    a_val = a_values[i]
    b_val = b_values[i]
    val = a_val*num + b_val
    val = val % 137
    binary_val = bin(val)
    binary_string = str(binary_val)
    return countZeroes(binary_string)

def combine(all_R_values):
    means = [0]*4
    for i in range(0, 4):
        sum = 0
        for j in range(i * 24, i * 24 + 24):
            sum += all_R_values[j]
        means[i] = sum / 24
    return median(means)

def FlajoletMartin(city_tuples):
    diff_cities=set()
    for pair in city_tuples:
        cityName=pair[0]
        cityNumber = int(binascii.hexlify(cityName.encode('utf8')), 16)
        diff_cities.add(cityName)
        for i in range (0,96):
            zeroes=getTailZeroes(cityNumber,i)
            if(zeroes>maxZeroes_for_hashFuncs[i]):
                maxZeroes_for_hashFuncs[i]=zeroes

    for i in range(0,96):
        all_R_values[i] = 2 ** maxZeroes_for_hashFuncs[i]

    uniqueCitiesEstimate = combine(all_R_values)
    true_uniqu_cities = len(diff_cities)
    curr_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    f = open(output, "a")
    f.write(str(curr_time) + ", " + str(true_uniqu_cities) + ", " + str(uniqueCitiesEstimate)+"\n")
    f.close()
    print("The actual and estimated values are : ", true_uniqu_cities, ", ", uniqueCitiesEstimate)
    return []

def writeFileHeaders(output):
    f = open(output, "w")
    f.write("Time, Ground Truth, Estimation")
    f.write("\n")
    f.close()

if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("check")
    port = int(sys.argv[1])
    output = sys.argv[2]
    writeFileHeaders(output)
    a_values = getAValues(96)
    b_values = getBValues(96)
    maxZeroes_for_hashFuncs = [0] * 96
    all_R_values=[0] * 96
    incomingStream = ssc.socketTextStream("localhost", port)
    cities = incomingStream.map(lambda x: json.loads(x)).map(lambda x: (x["city"],1))
    result = cities.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30,10).partitionBy(1).mapPartitions(FlajoletMartin)
    result.pprint()
    ssc.start()
    ssc.awaitTermination()