import myspark
from myspark import spark as spark
df = spark.read.csv('./data/test1.csv')
df.show(3)
df.count()
# 변수를 돌려주는 함수지 출력하는 함수가 아니다.
