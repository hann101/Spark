import myspark
from myspark import spark as spark

df = spark.read.csv('./data/fin.csv', header = True)

df2 = df.sample(0.01)

# 데이터를 처음 받았을 때 확인을 하고 싶을 때 샘플을 받아서 볼 수 있다.
# 중간에 있는 데이터를 확인할 수  잇느다.

df2.select('이름','핸드폰').show(5)
print(f'\n\tdf2 count:{df2.count()}\n')
# 40만건이면 1%만 넣는 것이다.. 4천건 ->4093건나옴..
# 샘플링에서 확률적으로 뽑기 때문에  다른 건수가 나온다.

df3 = df.sample(0.01)
df3.select('이름','핸드폰').show(5)
print(f'\n\tdf3 count: {df3.count()}')