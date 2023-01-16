'''
특정 기상 관측소의 최저 기온 구하기

Data Sample
관측소ID, 날짜, 기온 유형, 기온 등
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
'''
from pyspark import SparkConf, SparkContext


def parseLine(line):
    field = line.split(",")
    stationID = field[0]
    entryType = field[2]
    temperature = float(field[3]) * 0.1 * (9.0 / 5.0) + 32.0  # 화씨 변환
    return (stationID, entryType, temperature)


conf = SparkConf().setMaster("local").setAppName("minTemperature")
sc = SparkContext(conf=conf)

lines = sc.textFile("../Data/1800.csv")
parseLines = lines.map(parseLine)

minTemps = parseLines.filter(lambda x: "TMIN" in x[1])
# Create pair rdd (key: stationID, value: temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# Find Stations's Minimum temperature 
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))

for result in minTemps.collect():
    print(f"Station: {result[0]}, Temperature: {round(result[1], 2)}")


