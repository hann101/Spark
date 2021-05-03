import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

df = spark.read.csv('./data/fin.csv', header = True)

df = df.withColumn('income_avg', df['income_avg'].cast('int'))
df.select(
    F.percentile_approx('income_avg',0.5,1).alias('accuracy1'),
    # 위치에 있는 것들을 박구는 것이다. 
    # 1은 뭘까... 그 숫자에 대한 ㄴ정확한 내용을 나도 ㅇ모르겠어요`~~~~
    # 정확도가 1인 상태로 여러번 돌리면 다른 값이 나올 수 있다. 
    # 대충 그즘~ 정도라고 생각하면 될 것이다. 
    # F. 도 dataframe을 돌려주는 값이다. 근데 상화엥 따라 다르게 진행하기도 한다. 

    F.percentile_approx('income_avg',0.5,100).alias('accuracy100'),
    F.percentile_approx('income_avg',0.5,10000).alias('accuracy10000'), 
    F.percentile_approx('income_avg',[0.25,0.5,0.75]).alias('accuracy_quarter')
).show()
