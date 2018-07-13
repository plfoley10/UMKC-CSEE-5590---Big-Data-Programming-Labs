import os
from pyspark import SparkContext

os.environ["SPARK_HOME"] = "C:\\Users\\plfoley\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\Users\\plfoley\\winutils"

def sortKeys(keyvalue):
    w = keyvalue.split(' ')
    z = []
    for character in w:
        number = int(character)
        if not z:
            z.append(number)
        elif (z[0] > number):
            z.insert(0, number)
        else:
            z.append(number)

    str1 = ','.join(str(e) for e in z)

    return str1

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    lines = sc.textFile("C:\\Users\\plfoley\\PycharmProjects\\BigDataLab3\\facebook_combined.txt", 1)
    simplify = lines.map(lambda x: (sortKeys(x),(x.split(' '))[1:]))\
        .reduceByKey((lambda x, y: x + y))
    simplify.saveAsTextFile("output")
    sc.stop()