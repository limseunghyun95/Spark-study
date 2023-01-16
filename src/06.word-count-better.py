'''
텍스트 파일에서 단어의 빈도수 구하기 (개선한 버전)
'''
import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r"\W+", re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("wordCountBetter")
sc = SparkContext(conf=conf)

lines = sc.textFile("../Data/book.txt")
words = lines.flatMap(normalizeWords)
wordCount = words.countByValue()  # Type: 'collections.defaultdict'

for word, count in sorted(wordCount.items(), key=lambda x: x[1], reverse=True):  # 값을 기준으로 내림차순 정렬
    cleanWord = word.encode("ascii", "ignore")
    if (cleanWord):  # � 같은 데이터 출력에서 제외
        print(f"Word: {word}, Count: {count}")
