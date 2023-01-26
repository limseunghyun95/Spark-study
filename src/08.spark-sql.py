# SparkSession 에 Spark SQL 을 사용할 수 있는 인터페이스가 포함됨
from pyspark.sql import SparkSession, Row


def mapper(line):
    """
    csv의 한 행을 읽고 ',' 기준으로 분리한 후 열의 이름을 부여한 Row 로 반환
    csv 안에는 헤더가 없어서 지정하기 위함.
    """
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")),
               age=int(fields[2]), numFriends=int(fields[3]))


# Spark Context 와 유사한 개념으로 Spark Session 생성
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

# Session 안의 SparkContext 를 이용하여 파일을 읽음, RDD 로 읽기 위함
lines = spark.sparkContext.textFile("../Data/fakefriends.csv")
people = lines.map(mapper) # RDD

# RDD 를 DataFrame 으로 변환하고, 메모리에 적재하기 위해 cache() 사용
schemaPeople = spark.createDataFrame(people).cache()
# SQL 질의를 위해 데이터프레임으로 가상의 뷰를 생성
schemaPeople.createOrReplaceTempView("people") # 뷰의 이름은 people

# 데이터 중 청소년 데이터만 추출
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)  # SQL 에 맞는 결과가 Row로 출력됨

# 나이별 카운트를 세고 출력에는 나이를 오름차순으로 정렬하여 출력
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()  # Spark Session 종료 # 데이터 베이스 세션 여는 것과 동일
