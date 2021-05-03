import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

df = spark.read.csv('./data/fin.csv', header = True)
df = df.withColumn('income_avg', df['income_avg'].cast('int'))
# incom_avg라는 컬럼에 대한 데이터 분석을 보여준다.
df.select(
    F.lit(df.count()).alias('count1'),
    F.count('income_avg').alias('count2'),
    F.min('income_avg').alias('min'),
    # 앞에 있는놈을 뒤에 있는 놈으로 바꿔라!
    # 정수나 인수로 바꿔주는 것이 좋음
    F.max('income_avg').alias('max'),
    # max데이터를 읽어서 처리하면 string 그래서 max값이 9로 시작
    # string은 사전적의미로 인해 9가 가장 큰숫자이다. 
    F.avg('income_avg').alias('avg'),
    F.sum('income_avg').cast(DecimalType(38,0)) .alias('sum')
).show()

    # StructField('c0',StringType(),False),
    #     StructField('c0',StringType(),False),
    # StructField('c1',StringType(),False),
    # StructField('c2',IntegerType(),False),
    # StructField('c3',DoubleType(),False),
    # StructField('c4',DoubleType(),False),
    # StructField('c5',DoubleType(),False),
    # StructField('c6',StringType(),False),
    # StructField('c7',DoubleType(),False)