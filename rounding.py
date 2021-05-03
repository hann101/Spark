import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

round__ = F.udf(lambda x, step:round(x,step))
ceil__ = F.udf(lambda x, step: step*math.ceil(x /step))
floor__ = F.udf(lambda x, step: step*math.floor(x / step))
trunc__ = F.udf


# 반올림 올림 내님




# 0.1을 넣으면 소수점 첮제자리에 적용이된다. 