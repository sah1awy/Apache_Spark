from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster('local').setAppName("Customer")
sc = SparkContext.getOrCreate(conf=conf)
def customized_split(x):
    x = x.split(',')
    return (int(x[0]),float(x[2]))
 
lines = sc.textFile('customer-orders.csv')
rdd = lines.map(customized_split)
rdd = rdd.reduceByKey(lambda x,y: x+y)
count = rdd.sortBy(lambda x: x[1])
res = count.collect()
for r in res:
    print(r)

count2 = rdd.map(lambda x: (x[1],x[0]))
count2 = count2.sortByKey()
res2 = count2.collect()
for r in res2:
    if r[1] > 9:
        print("Customer ID:",r[1],'\tTotal Spend:',round(r[0],2))
        print('-'*100)
    else:
        print("Customer ID:",r[1],' '*8,'Total Spend:',round(r[0],2))
        print('-'*100)