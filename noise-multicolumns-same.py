import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
# F는 컬럼형태로 리턴한다. 
from datetime import datetime
from datetime import timedelta

# 노이즈를 추가하면 램덤 날짜의 

df = spark.read.csv('./data/med.csv', header=True)
df = df.withColumn('noise',F.round(F.rand()*6).cast('int')-3)

@F.udf('date')
def date_add__(x,y):
    return datetime.strptime(x, '%Y-%m-%d')
