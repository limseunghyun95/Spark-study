from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

'''
fakefriends-header.csv: 헤더가 있는 SNS Data

userID,name,age,friends
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
3,Deanna,40,465
...

spark.read 로 spark.Context 대신 데이터를 읽을 수 있다.
option("header", "true"): 헤더가 포함된 데이터  여부
option("inferSchema", "true"): 데이터의 스키마를 파악여부
csv("<Data Path>"): csv 파일 읽기
'''
people = spark.read.option("header", "true").option("inferSchema", "true").csv("../Data/fakefriends-header.csv")
print(type(people)) # pyspark.sql.dataframe.DataFrame

'''
inferSchema 옵션으로 찾은 데이터 스키마 출력
'''
print("Here is our inferred schema:")
people.printSchema()

'''
DataFrame.select("<Fields>"): 특정 필드의 값을 추출, "," 을 이용하여 여러 필드들을 지정할 수 있음
show(): 출력, Default 는 상위 20개의 row만 출력, 지정한 값만큼 출력 가능
'''
print("Let's display the name column:")
people.select("name", "age").show()

'''
DataFrame.filter(Dataframe.column 조건): 특절 필드 조건에 해당하는 값만 추출
    - &: and, |: or, ~: not 을 이용하여 여러 조건을 사용
    - 단, 여러 조건을 이용시에는 각 조건마다 () 괄호로 구성되어야함
show(): 출력
'''
print("Filter out anyone over 21:")
people.filter(people.age < 21).show() # 나이 21세 이하의 데이터만 출력
people.filter(people.age < 21).select("name", "age").show() # 나이 21세 이하의 이름과 나이만 출력
people.filter((people.age < 21) & (people.age > 18)).select("name", "age").show()

'''
DataFrame.groupBy(DataFrame.column): 해당 필드로 데이터를 그룹화
    - DataFrame.groupBy("age").count(): age 필드의 각 값마다 빈도수를 구함
'''
print("Group by age:")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()
