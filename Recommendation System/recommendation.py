from pyspark import SparkContext
import math
import sys
import time
import itertools
from collections import defaultdict

def possibleCombinations(hashes):
    poss_cand_pairs=[]
    for bus_list in hashes.values():
        if len(bus_list)>=2:
            for each in itertools.combinations(bus_list,2):
                poss_cand_pairs.append(each)
    return poss_cand_pairs

def calculateHash(a,b,j,m):
    return ((a * j) + b) % m

def avrage_for_current_business(current_business):
    sum=count=0
    for user in bus_user_list[current_business]:
        count=count + 1
        sum=sum+data[user][current_business]
    average=0.0
    average=sum/count
    return average

def average_rating_for_user(current_user):
    sum=count=0
    for business in user_bus_list[current_user]:
        count=count + 1
        sum=sum+data[current_user][business]
    average=0.0
    average=sum/count
    return average

def check_if_zero(d1,d2):
    if(d1<=0 or d2<=0):
        return True
    return False

def getNewAverage(current_business,common_user,curr_avg,l):
    rating=data[common_user][current_business]
    new_score=curr_avg*l
    new_score=new_score-rating
    l=l-1
    return new_score/l

def predict(current_user,current_business):
    if current_user not in user_bus_list.keys() and current_business in bus_user_list.keys():
        return avrage_for_current_business(current_business)
    elif current_business not in bus_user_list.keys() and current_user in user_bus_list.keys():
        return average_rating_for_user(current_user)
    elif current_business not in bus_user_list.keys() and current_user not in user_bus_list.keys():
        return 3
    else:
        similarity_score={}
        users_co_rated=bus_user_list[current_business]
        averages={}
        for user in users_co_rated:
            average_rating=average_rating_for_user(user)
            averages[user]=average_rating
        averages[current_user]=average_rating_for_user(current_user)

        for other_user in users_co_rated:
            d1=0
            d2=0
            n=0
            common_businesses=set(user_bus_list[current_user])&set(user_bus_list[other_user])
            for common_business in common_businesses:
                d1=d1+(data[current_user][common_business] - averages[current_user])*(data[current_user][common_business] - averages[current_user])
                d2=d2+(data[other_user][common_business] - averages[other_user])*(data[other_user][common_business] - averages[other_user])
                n=n+(data[other_user][common_business] - averages[other_user])*(data[current_user][common_business] - averages[current_user])
            if(check_if_zero(d1,d2)):
                similarity_score[other_user]=0.1
            else:
                similarity_score[other_user]=math.sqrt((n*n)/(d1*d2))

        n=0
        d=0
        for common_user in users_co_rated:
            average_excluding_item=getNewAverage(current_business,common_user,averages[common_user],len(user_bus_list[common_user]))
            n=n+(data[common_user][current_business] - average_excluding_item) * similarity_score[common_user]
            d=d+abs(similarity_score[common_user])

        if check_if_zero(d,1):
            return 3
        else:
            predicted_value=averages[current_user]+n/d
            if predicted_value<=5:
                return predicted_value
            else:
                return 5.0

start=time.time()
sc=SparkContext('local[*]', 'task2-darshan')

training=sys.argv[1]+"yelp_train.csv"
val=sys.argv[2]
output_file=sys.argv[3]

training_data=sc.textFile(training).map(lambda a:a.split(",")).filter(lambda a:a[0]!='user_id')
val_data=sc.textFile(val).map(lambda a:a.split(",")).filter(lambda a:a[0]!='user_id')


data=training_data.map(lambda a: (a[0], (a[1], float(a[2])))).groupByKey()
data=data.map(lambda x: (x[0], dict(x[1]))).collectAsMap()
user_bus_list=training_data.map(lambda a: (a[0], a[1])).groupByKey()
user_bus_list=user_bus_list.map(lambda a: (a[0], list(a[1]))).collectAsMap()
bus_user_list=training_data.map(lambda a: (a[1], a[0])).groupByKey()
bus_user_list=bus_user_list.map(lambda a: (a[0], list(a[1]))).collectAsMap()
predictions=val_data.map(lambda a: (a[0],a[1],float(a[2]),predict(a[0], a[1]))).collect()
# print(predictions)
f=open(output_file,"w")
f.write("user_id, business_id, prediction \n")
for row in predictions:
    f.write(row[0]+","+row[1]+","+str(row[3])+"\n")
f.close()

all = []
errors = defaultdict(int)
for each in predictions:
    all.append((each[2] - each[3]) ** 2)

    if each[3] - each[2] <= 1:
        errors[1] += 1
    elif each[3] - each[2] <= 2:
        errors[2] += 1
    elif each[3] - each[2] <= 3:
        errors[3] += 1
    elif each[3] - each[2] <= 4:
        errors[4] += 1
    else:
        errors[5] += 1

RMSE=math.sqrt(float(sum(all)) / len(all))

print("RMSE:",RMSE)
print("Error:",errors)

end = time.time()
print("Duration: ",end-start)






