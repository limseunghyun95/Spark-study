from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession. \
    builder. \
    appName("MostPopularSuperHero"). \
    master("local"). \
    getOrCreate()

schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("name", StringType(), True)
])


names = spark.read. \
    schema(schema). \
    option("sep", " "). \
    csv("../Data/Marvel-Names")
lines = spark.read.text("../Data/Marvel-Graph")

'''
    func.split(func.col("필드"), 구분자): 데이터 프레임의 필드의 값을 구분자 기준으로 나눔
    dataframe.withColumn("필드명", 칼럼): 해당 칼럼을 필드명으로 데이터 프레임에 추가 
    func.size(필드): 해당 필드의 길이를 구함
    gropuBy(필드): 필드의 값들로 그루핑
    agg(func.집계연산(필드).alias(신규 필드명)): 해당 필드값에 대해 집계연산을 수행하고 그 수행결과를 신규 필드명으로 지정
    sort(func.col("필드")): 필드명으로 오름차순 정렬 / func.col("필드").desc(): 내림차순 
'''
connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections")) \
    .sort(func.col("connections").desc())

mostPopular = connections.first()  # Row

''' 
    dataframe.filter(func.col("필드") == 값): datafrmae의 필드값과 값이 일치하는 것만 필터링
'''
mostPopularName = names.filter(func.col("id") == mostPopular[0])  # DataFrame
mostPopularName.show()

spark.stop()
