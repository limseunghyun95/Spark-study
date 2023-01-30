from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("minTemperature").getOrCreate()

'''
읽고자하는 데이터는 헤더가 없는 CSV 파일이다.
spark.sql로 활용하기 위해 데이터에 대한 스키마를 제시한다.

SturctType([
    StructField("<field name>", pyspark.sql.type, nullable), ...
])
- pysark.sql.type 은 pyspark.sql.types 하위에 있는 클래스들을 중에서 불러오면 된다.

spark.read.schema(customSchema) 으로 사전 정의된 스키마를 적용한다.
'''
schema = StructType([StructField("stationID", StringType(), True),
                    StructField("date", IntegerType(), True),
                    StructField("measure_type", StringType(), True),
                    StructField("temperature", FloatType(), True)])
df = spark.read.schema(schema).csv("../Data/1800.csv")
'''
커스텀 스키마가 잘 적용된 모습
root
 |-- stationID: string (nullable = true)
 |-- date: integer (nullable = true)
 |-- measure_type: string (nullable = true)
 |-- temperature: float (nullable = true)
'''
df.printSchema()

# 관측 대상이 최저 온도인 것만 필터링
minTemps = df.filter(df.measure_type == "TMIN")

# 필요한 필드인 관측소ID, 온도만 추출
stationTemps = minTemps.select("stationID", "temperature")

# 각 관측소별 최소 온도만 추출
minTempBystation = stationTemps.groupBy("stationID").min("temperature")

# 최소 온도 필드의 값을 화씨로 변경하고 출력
minTempByStationF = minTempBystation.withColumn("temperature", func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2)
                                                ).select("stationID", "temperature").sort("temperature").show()

spark.stop()
