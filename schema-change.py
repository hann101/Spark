import myspark
from myspark  import spark as spark

df = spark.read.csv(
    path = './data/test1.csv',
    header = False
    )

df = df.withColumnRenamed('_c0','c0')
df = df.withColumnRenamed('_c1','c1')
df = df.withColumnRenamed('_c2','c2').withColumn('c2',df['c2'].cast('int'))
df = df.withColumnRenamed('_c3','c3').withColumn('c3',df['c3'].cast('double'))
df = df.withColumnRenamed('_c4','c4').withColumn('c4',df['c4'].cast('double'))
df = df.withColumnRenamed('_c5','c5').withColumn('c5',df['c5'].cast('double'))
df = df.withColumnRenamed('_c6','c6')
df = df.withColumnRenamed('_c7','c7').withColumn('c7',df['c7'].cast('double'))
#  _c0을 c0으로 바꾸고
# c2의 c2라는 컬럼을 int로 캐스팅해라
# 컬럼의 값(str)을 \int로 바꾸는 것ㅇ..
# 업는 컬럼은 새로 생성하게 된다. 

df.printSchema()

df.write.mode('overwrite').parquet('out2')
# 파일에서 파켓으로 내보내는 기능이 있는지 확인하는 것.
# 중간파일 처리할때 parquet을 사용하면 편하다. 