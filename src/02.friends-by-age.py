from pyspark import SparkConf, SparkContext


def parseLine(line) -> tuple:
    """
    comma로 구분된 line RDD 에서 필요한 데이터를 추출
    Key/Value RDD 형태로 생성

    Args:
        line (_type_): line RDD

    Returns:
        tuple: (age, number of friends)
    """
    field = line.split(",")
    age = int(field[2])
    numFriends = int(field[3])
    return (age, numFriends)


conf = SparkConf().setMaster("local").setAppName("SNSCounter")
sc = SparkContext(conf=conf)

lines = sc.textFile("../Data/fake_sns.csv")
rdd = lines.map(parseLine)  # [(33, 385), (33, 2), (55, 221), (40, 465), (68, 21)]

'''
rdd.mapValues(lambda x: (x,1)): 키-값 쌍에서 값에 1을 추가하여 평균을 구할 때, 키의 갯수로 사용하기 위한 전력
>>> (33,385) -> (33, (385, 1))
>>> (33,2) -> (33, (2, 1))

reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])): 키-값 쌍에서 같은 키를 대상으로 값을 합산
>>> (33, (385, 1)), (33, (2, 1)) -> (33, (387, 2))
'''
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  # [(33, (387, 2)), (55, (221, 1)), (40, (465, 1)), (68, (21, 1))]

'''
mapValues(lambda x: x[0] / x[1]): 키를 유지하면서 값의 0 번째 값(친구의 총합)과 1 번째 값(키의 갯수)를 통해 평균을 계산
>>> (33, (387, 2)) -> (33, 193.5)
'''
averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]) # [(33, 193.5), (55, 221.0), (40, 465.0), (68, 21.0)]
for result in averageByAge.collect():
    print(f"Age: {result[0]} Average of friends num: {result[1]}")