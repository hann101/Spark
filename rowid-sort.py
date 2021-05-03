import myspark
from myspark import spark as spark

from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

df = spark.read.csv('./data/fin.csv', header = True)

# orderby는 순서대로 하게 ㅆ다는 의미이 앋. 
# withcolullmn 추가해주는 것 idx라는 컬럼을 추가 해주는 것읻ㅁ

window = W.orderBy(F.col('일련번호'))
df = df.withColumn('idx',F.row_number().over(window))
df.select('idx','일련번호').show()