import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import math 

# 범주화 -> 로컬 일반화
#  같을 먼저 알고 있어야 된다. 코딩을 일반화해서 하기 힘들다. 
#  특이치를 찾으면 협의를 하고 코딩을 해야한다. 
df = spark.read.csv('./data/med.csv', header = True )
df = df.withColumn('체중',df['체중'].cast('int'))
# 갯수를 기준으로 정순만 보게 된다. 

df1 = df.groupby('체중').count()
df1.orderBy('count').show()

weight_10__ = F.udf(lambda x : math.floor(x / 10) * 10, 'int')
# 범주화를 진행하는데, 내림으로 한다. 
#  string으로 바뀌는 경우가 잇는 데.바꾸지 않고 내림을 하고 있다. 
# 0~10까지 0으로 나오고 10~19까지는 10으로 나오게 해서 범주화를 유지 하고 잇따. 
df2 = df.withColumn('weight_10', weight_10__(F.col('체중')))


# 3
df3 = df2.groupby('weight_10').count()
df3.orderBy('count').show()


weight_local__ = F.udf(lambda x : '100kg이상' if x > 100 \ 
else '30kg 이하' if x<40 \
else str(x), 'string')
# 들어온값은 int 형이다. 그래서 오류남.. str로 바꿔서 리턴하도록하자. 
# 문자열의 정렬과 숫자열을리 정렬릉 ㄴ다른 수 박ㄲ에 없다. 

df4 = df2.withColumn('weight_local', weight_local__(F.col('weight_10')))
df4.groupby('weight_local').count().orderBy('weight_local').show()
