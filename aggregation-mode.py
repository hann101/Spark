import myspark
from myspark import spark as spark 
# 최빈  -> 빈도수 
from pyspark.sql import functions as F

df = spark.read.csv('./data/fin.csv', header = True)

df = df.withColumn('income_avg',df['income_avg'].cast('int'))

df1 = df.groupBy('income_avg').count()
# income_avg의 값들을가지고 그룹화를 해라. 그룹화한 애들만 따로 모으고 그 갯수를 세서
# df1에 넣는 것이다.근데 순서대로 정리가 안되 있다.
#  sort도 사용할 수 있다.
df1 = df1.orderBy(F.col('count').desc())
#  count라는 컬럼을 정렬해라. desc을 사용하면 역순으로 정렬을 하게 됨.
df1.show()
# 스파크의 가장 큰 특징.. 명령어 를 기억 해 두었다가.. show/count같은 실제 행위가 있기전에
# 아무 것도 하지 않는다.. 컴터 입장에서는 효율적인 행동.
# 특별한 경ㅇ우가아니면 정렬을 할 필요없

df1.limit(1).select('income_avg').show()
# 최빈 값을 뽑는 것이니, 위에 있는 것을 콕 집어서 뽑는 것이다. 
# 최대 한게 ㄲㅏ지만 뽑아라. 매우 유용
# 사례 백만개가 있으면 한개만 뽑으면 속도가 훨씬 빠르다. 


print(f'\n\tmobe value:{df1.limit(1).collect()}')
# collect를 사용하면 리스트형태로 반환해준다. 
# 2차원 배열로 나오게 된다. 

