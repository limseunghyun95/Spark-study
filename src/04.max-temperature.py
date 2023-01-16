'''
관측소 별 최대 온도 구하기
'''
from pyspark import SparkConf, SparkContext


def parseline(line):
    field = line.split(",")
    stationID = field[0]
    entryType = field[2]
    temperature = float(field[3]) * 0.1 * (9.0 / 5.0) + 32  # 화씨 변환
    return (stationID, entryType, temperature)


conf = SparkConf().setMaster("local").setAppName("maxTemperature")
sc = SparkContext(conf=conf)


lines = sc.textFile("../Data/1800.csv")
parselines = lines.map(parseline)

maxEntry = parselines.filter(lambda x: "TMAX" in x[1])
stationTemp = maxEntry.map(lambda x: (x[0], x[2]))
maxTemp = stationTemp.reduceByKey(lambda x, y: max(x, y))

for result in maxTemp.collect():
    print(f"Station: {result[0]}, Temperature: {round(result[1], 2)}")
