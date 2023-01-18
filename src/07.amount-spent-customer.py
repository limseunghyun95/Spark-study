'''
고객별 총 지출한 금액 구하기
'''
from pyspark import SparkConf, SparkContext


def parseLine(line):
    field = line.split(",")
    customerID = int(field[0])
    spent = float(field[2])

    return (customerID, spent)


conf = SparkConf().setMaster("local").setAppName("amountSpentCustomer")
sc = SparkContext(conf=conf)

lines = sc.textFile("../Data/customer-orders.csv")
customerSpent = lines.map(parseLine)
customerTotal = customerSpent.reduceByKey(lambda x, y: x + y)
customerTotal = customerTotal.sortBy(lambda x: -x[1]) # 값을 기준으로 내림차순 정렬

for result in customerTotal.collect():
    print(f"ID: {result[0]}, Total: {round(result[1], 2)}")
