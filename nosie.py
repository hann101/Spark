import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

import random
from datetime import datetime
from datetime import timedelta

# 스트링을 데이트타임형태로 바꿔즌다.
@F.udf('date')
def add_date(x):
    date = datetime.strptime(x, '%Y-%m-%d')
    return date + timedelta(days=random.randrange(-3,3))
    # time델타라는 모듈을 가지고 있다.
    # 스트링에 time델타를 더하는 것이다. 이것은 월일 수 있다. 
    # days는 일 을 기준으로 랜덤으로 ㅆ느는 것이다. 

df = spark.read.csv('./data/med.csv', header = True)
df = df.withColumn('진단일자', df['진단일자'].cast('date'))
df = df.withColumn('date_1',F.date. _add(df['진단일자'],random.randrange(-3,3)))
df.select('진단일자', 'date_1').show()

# 날짜를 랜덤으로 뽑고 그다으므에 수샂 컬럼을 추가하낟. 


df = spark.read.csv('./data/med.csv', header = True)
df = df.withColumn('진단일자', df['진단일자'].cast('date'))
df = df.withColumn('date_1',F.date. _add(df['진단일자'],random.randrange(-3,3)))
df.select('진단일자', 'date_1').show()

