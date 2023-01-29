from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("../Data/fakefriends-header.csv")

'''
각 나이별 평균 친구의 수를 구하기
'''
people.groupBy("age").avg("friends").sort("age").show()

'''
각 나이별 평균 친구의 수를 구하기
출력 조건
1. 평균 친구의 수는 소수점 2자리 까지만 표시. 그 이하는 반올림

pyspark.sql.functions.avg(필드): 해당 필드의 평균을 구함
pyspark.sql.fucntions.round(값, n): 값의 n번째 소수점 이하를 소수점을 반올림
DataFrame.agg(n): GroupBy 한 전체 결과를 집계
.alias(n): 출력시 n 라는 이름으로 출력. Like MySQL AAS
'''
people.groupBy("age").agg(func.round(func.avg("friends"), 2)
                          .alias("friends_avg")).sort("age").show(100)

spark.stop()
