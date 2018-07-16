import os
from pyspark import SparkContext

os.environ["SPARK_HOME"] = "C:\\Users\\plfoley\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\Users\\plfoley\\winutils"

def sortKeys(keyvalue1, keyvalue2):
    splitvalues = keyvalue2.split(' ')
    temp = []
    if int(splitvalues[0]) > int(keyvalue1):
        temp.append(int(keyvalue1))
        temp.append(int(splitvalues[0]))
    else:
        temp.append(int(splitvalues[0]))
        temp.append(int(keyvalue1))
    str1 = ','.join(str(e) for e in temp)
    return str1

def keepDup(x, y):
    w = x + y
    duplist = set([x for x in w if w.count(x) > 1])
    if not duplist:
        duplist=['No Mutual Friends']
    return duplist

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    lines = sc.textFile("C:\\Users\\plfoley\\PycharmProjects\\BigDataLab3\\facebook_combined.txt", 1)
    simplify = lines.flatMap(lambda x: [(sortKeys(w,x), (x.split(' '))[1:]) for w in (x.split()[1:])])\
        .reduceByKey((lambda x, y: keepDup(x,y)))
    simplify.saveAsTextFile("output")
    sc.stop()