import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
from Cryptodome.Hash import SHA256

DATA_ROOT = './data'
JOIN_KEY_COLUMN = 'joinkey'
ROW_ID_COLUMN = 'rowid'
SALT = b'Salt is salty..'
KEY_COLUMNS = ['생년월일', '시도', '핸드폰', '성별', '이름']

customers = ['finance']

str_slice_first_4 = F.udf(lambda x: '' if x is None else x[:4])
# x가 none이면 ''를 반환, 그게아니면 4번째까지 긁어
str_slice_last_4 = F.udf(lambda x: '' if x is None else x[-4:])
str_plus_B = F.udf(lambda x: 'B'+str(x))

@F.udf('string')
def hash(msg):
    h = SHA256.new(SALT)
    h.update(msg.encode('utf-8'))
    return h.hexdigest()

def make_key_file(customer):
    df = spark.read.csv(f'{DATA_ROOT}/{customer}.csv', header=True, encoding='euc-kr')

    df = df.withColumn('생년월일', df['생년월일'].cast('string'))
    df = df.withColumn('핸드폰', df['핸드폰'].cast('string'))

    df = df.withColumn('생년월일', str_slice_first_4(F.col('생년월일')))
    df = df.withColumn('핸드폰', str_slice_last_4(F.col('핸드폰')))

    df = df.withColumn(JOIN_KEY_COLUMN, hash(F.concat_ws('|', *KEY_COLUMNS)))
    df = df.withColumn(ROW_ID_COLUMN, F.monotonically_increasing_id())
    df = df.withColumn(ROW_ID_COLUMN, str_plus_B(F.col(ROW_ID_COLUMN)))

    dfkey = df.select(JOIN_KEY_COLUMN, ROW_ID_COLUMN)
    dfkey.coalesce(1).write.mode('overwrite').csv(f'{DATA_ROOT}/balance/{customer}_key', header=True)

    dfdata=df.drop(JOIN_KEY_COLUMN, '핸드폰', '이름')
    dfdata.write.mode('overwrite').csv(f'{DATA_ROOT}/balance/{customer}_data', header=True)

for customer in customers:
    make_key_file(customer)