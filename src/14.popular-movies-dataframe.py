from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

spark = SparkSession.builder.master("local").appName("popularMovies").getOrCreate()

schema = StructType([StructField("userID", IntegerType(), True),
                     StructField("movieID", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])  # bigint 는 LongType으로

df = spark.read.option("sep", "\t").schema(schema).csv("../Data/ml-100k/u.data")

# 가장 리뷰수가 많은 데이터
popularMovies = df.groupBy("movieID").count().orderBy(func.desc("count"))

# 전체 조회
# popularMovies.show(popularMovies.count())
popularMovies.show(20)

# 평점이 가장 높은 데이터
topRateMovies = (df.groupBy("movieID").
                 agg(func.round(func.avg("rating"), 2).alias("rating_average")).
                 orderBy(func.desc("rating_average")).show())

spark.stop()
