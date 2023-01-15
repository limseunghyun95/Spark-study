"""
평점별 히스토그램 생성
"""
from collections import OrderedDict
# SparkConf: 스파크 컨텍스트를 설정하는 객체
from pyspark import SparkConf, SparkContext

# setMaster(): 마스터 노드를 설정하는 것으로 "local"로 설정하여 클러스터가 아니라 로컬 머신에서 실행하겠다는 것으로 설정
# setAppName(): 애플리케이션 이름. 웹 UI에서 AppName 을 통해 애플리케이션을 구분
conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf=conf)

# sc.textFile: 데이터를 로드하는데 한 줄씩 쪼개서 RDD 를 생성
lines = sc.textFile("../Data/ml-100k/u.data")
# RDD 각 모든 줄에 평점만 추출해서 rating 이라는 RDD 생성 (변환)
ratings = lines.map(lambda x: x.split()[2])
# rating을 Grouping 해서 각 평점 당 갯수를 구함 (Action)
result = ratings.countByValue()

# 결과를 출력
sortedResults = OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f"{key} {value}")
