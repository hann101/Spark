import findspark
findspark.init()

from pyspark.conf import SparkConf
from pyspark.context import SparkConf
from pyspark.sql.session import SparkSession

spark = SparkSession.builder  \
    .master('local')\
    .appName('timecandy')\
    .config('spark.driver.memory','10g')\
    .getOrCreate()
