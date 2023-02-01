from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.master("local").appName("totalSpentCustomer").getOrCreate()

schema = StructType([StructField("customerID", IntegerType(), True),
                     StructField("itemID", IntegerType(), True),
                     StructField("cost", FloatType(), True)])

df = spark.read.schema(schema).csv("../Data/customer-orders.csv")

'''
고객별 총 구매액을 계산
'''
totalSpentCustomer = df.groupBy("customerID"). \
    agg(func.round(func.sum("cost"), 2).alias("totalSpent")). \
    sort("totalSpent")
totalSpentCustomer.show(totalSpentCustomer.count())

spark.stop()
