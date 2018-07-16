from pyspark import *
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark import SparkContext
from pyspark.sql.functions import col
import os

os.environ["SPARK_HOME"] = "/Users/jm051781/Documents/spark-2.3.1-bin-hadoop2.7/"

spark = SparkSession \
    .builder \
    .getOrCreate()

##Lab 3 Part A.) Create DF from file, auto imports strcuts as Int,String
print("\nRead Data from File")
df = spark.read.load("/Users/jm051781/Documents/SchoolWork/CS490-LABS/Lab3/WorldCups.csv",
                        format="csv", sep=",", inferSchema="true", header="true")
#World Cup DF
df.show()
df.printSchema()

##Lab 3 Part B.) 10 Queries && Part C.) 5 does with Spark RDD DF instead of SQLContext Query
#1.DISTINCT drop Duplicates
print((df.count())-(df.dropDuplicates().count())) #Will be 0 no world cup has ever been the same. :)
#2.Won more than once
df.groupBy("Winner").count().show()
#3.Distinct # of winners
df.groupBy("Winner").count().where(f.col('count')>1).select(f.sum('count')).show()
#4.Distinct # of Runner-ups
df.groupBy("Runners-up").count().where(f.col('count')>1).select(f.sum('count')).show()
#5.Distinct # of Third && 4th Place
third = df.groupBy("Third").count().where(f.col('count')>1).select(f.sum('count'))
fourth = df.groupBy("Winner").count().where(f.col('count')>1).select(f.sum('count'))
unionDF = third.unionAll(fourth)
unionDF.registerTempTable("temp")
db = spark.sql("Select * from temp").show()
##Above was done using RDD Spark DF
##Remainder will be SparkSql Context'd
df.createOrReplaceTempView("WorldCup")
#6.Total goals Scored by year
sqlDB = spark.sql("SELECT Year,GoalsScored FROM WorldCup group by Year, GoalsScored").show(200)
#7. Avg Attendance asc.
sqlDB = spark.sql("SELECT AVG(Attendance) FROM WorldCup").show()
#8. Total Matches played throughout the years
sqlDB = spark.sql("SELECT SUM(MatchesPlayed) FROM WorldCup").show()
#9. Avg matches played per Year
sqlDB = spark.sql("SELECT AVG(MatchesPlayed), Year FROM WorldCup Group By MatchesPlayed, Year")
#10.Random (Fun)
print(sorted(df.agg(f.min(df.GoalsScored)).collect()))
print(sorted(df.agg(f.max(df.GoalsScored)).collect()))
#Min max of of goals scored in cupYear.
