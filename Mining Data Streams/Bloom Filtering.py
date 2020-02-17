from pyspark.context import SparkContext
import sys
from datetime import datetime
import binascii
import json
import os.path
import random
from pyspark.streaming import StreamingContext

def generateA():
    l1 = []
    for i in range(0, 5):
        l1.append(random.randint(10, 100))
    return l1

def generateB():
    l2 = []
    for i in range(0, 5):
        l2.append(random.randint(12, 70))
    return l2

def writeHeader(file_name):
    f = open(file_name, "w")
    f.write("Time, FPR\n")
    f.close()

def writeStatesToFile(visited):
    data_dict = {}
    data_dict["states"] = list(visited)
    file1 = open("states.json", "w")
    json.dump(data_dict, file1)
    file1.close()

def BloomFilter(input):
    if not(os.path.isfile('states.json')):
        visited = set()
    else:
        f = open("states.json","r")
        data = json.load(f)
        visited = set(data["states"])
    false_positive = 0
    true_negative = 0

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for each in input:
        state_number = int(binascii.hexlify(each[0].encode('utf8')), 16)
        count = 0
        for i in range (0,5):
            bit_position = (a[i] * state_number + b[i]) % 200
            if hashed_bit[bit_position] == 1:
                count = count + 1
            else :
                hashed_bit[bit_position] = 1
        if count == 5:
            if each[0] not in visited:
                false_positive += 1
        else:
            if each[0] not in visited:
                true_negative += 1
        visited.add(each[0])

    down = false_positive + true_negative

    if down == 0:
        false_pos_rate = 0.0
    else:
        false_pos_rate = false_positive /(false_positive+true_negative)
    file = open(file_output, "a")
    file.write(current_time + ", " + str(false_pos_rate) + "\n")
    file.close()
    writeStatesToFile(visited)

    return []

if __name__== "__main__" :
    if (os.path.isfile('states.json')):
        os.remove('states.json')
    sc = SparkContext.getOrCreate()
    stream_context = StreamingContext(sc, 10)
    file_output = sys.argv[2]
    p_num = int(sys.argv[1])
    a = generateA()
    b = generateB()
    hashed_bit = [0 for i in range (200)]
    writeHeader(file_output)
    incoming = stream_context.socketTextStream("localhost", p_num)
    states = incoming.map(lambda x: json.loads(x)).map(lambda x:(x["state"],1))
    answer = states.partitionBy(1)
    answer = answer.mapPartitions(BloomFilter)
    answer.pprint()
    stream_context.start()
    stream_context.awaitTermination()
