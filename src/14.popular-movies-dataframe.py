import codecs
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, LongType, IntegerType


def loadMovieNames():
    movieNames = {}
    with codecs.open("../Data/ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]

    return movieNames


spark = SparkSession.builder.master("local").appName("popularMovies").getOrCreate()

# Key가 ID고 Value가 이름인 딕셔너리 형태의 브로드케스트 변수 생성
"""
broadcast 변수 생성 방법

spark: SparkSession
spark.sparkcont.broadcast(브로드케스트할 값)
"""
nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([StructField("userID", IntegerType(), True),
                     StructField("movieID", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])  # bigint 는 LongType으로

df = spark.read.option("sep", "\t").schema(schema).csv("../Data/ml-100k/u.data")

# 가장 리뷰수가 많은 데이터
popularMovies = df.groupBy("movieID").count().orderBy(func.desc("count"))

# ID-Name 매핑 결과 반환 함수
def lookupName(movieID):
    return nameDict.value[movieID]


# 브로드캐스트 값으로 영화 ID 와 영화 제목 매핑
lookupNameUDF = func.udf(lookupName)

# 매핑한 영화이름을 DataFrame에 추가
moviesWithNames = popularMovies.withColumn("movietitle", lookupNameUDF(func.col("movieID")))

# 전체 조회
# popularMovies.show(popularMovies.count())
moviesWithNames.show(20)

# 평점이 가장 높은 데이터
topRateMovies = (df.groupBy("movieID").
                 agg(func.round(func.avg("rating"), 2).alias("rating_average")).
                 orderBy(func.desc("rating_average"))
                 .withColumn("movietitle", lookupNameUDF(func.col("movieID")))
                 .show())

spark.stop()
