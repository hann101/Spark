import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import random


@F.udf('int')
def sexcode(x):
    if x is None: return 90
    return random.randrange(10) + (10 if x =='M' else 0)
    # 특이한 값이 있는 지 확인을 하곡 처리를 해야한다.
    # 0~10보다 작은 수를 뽑는다. 정수로
    

df = spark.read.csv('./data/med.csv', header = True)
df.select('성별').distinct().show()

df = df.withColumn('sex',sexcode(F.col('성별')))
df = df.drop('성별').withColumnRenamed('sex','성별')
df.select('성별').distinct().orderBy('성별').show()