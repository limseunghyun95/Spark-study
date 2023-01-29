from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("wordCount").getOrCreate()

'''
spark.read.text("<Data Path>"): 텍스트 파일 읽어오기
- DataFrame 형태로 읽어온다.
'''
text = spark.read.text("../Data/book.txt")

'''
text는 구조화된 데이터가 아니기 때문에 한 줄은 한 행이 되며 그 필드는 기본값인 'value' 로 지정된다.
- text.value 를 통해 값을 불러올 수 있다.
func.explode(n): RDD의 FlatMap과 비슷한 역할로 배열 혹은 맵의 각 요소에 대해 Row 형태로 반환
func.split(n): 해당 값을 특정 패턴에 따라 구분짓는다.
    - Returns: pyspark.sql.column.Column
    - func.split(n, "\\W+"): 정규식을 활용한 것으로 문자를 기준으로 구분하겠다는 뜻
'''
words = text.select(func.explode(func.split(text.value, "\\W+")).alias("word"))
'''
words DataFrame 중 word 필드에서 공백은 제거
'''
words.filter(words.word != "")

'''
word 라는 필드의 각각의 행에 텍스트값을 소문자로 변환
'''
lowercaseWords = words.select(func.lower(words.word).alias("word"))

'''
word Count 출력
'''
lowercaseWords.groupBy("word").count().sort("count").show()

spark.stop()
