'''
텍스트 파일에서 단어의 빈도수 구하기
'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("../Data/book.txt")
words = lines.flatMap(lambda x: x.split())
wordCount = words.countByValue() # Type: 'collections.defaultdict'

for word, count in wordCount.items():
    cleanWord = word.encode("ascii", "ignore")
    if (cleanWord): # � 같은 데이터 출력에서 제외
        print(f"Word: {cleanWord}, Count: {count}")
        
        '''
        결과
        Word: you, Count: 1878
        Word: to, Count: 1828
        Word: your, Count: 1420
        Word: the, Count: 1292
        Word: a, Count: 1191
        Word: of, Count: 970
        Word: and, Count: 934
        Word: that, Count: 747
        Word: it, Count: 649
        Word: in, Count: 616
        Word: is, Count: 560
        Word: for, Count: 537
        '''