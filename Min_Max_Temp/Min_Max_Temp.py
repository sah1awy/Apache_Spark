from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("MinMaxTemperature")
sc = SparkContext.getOrCreate(conf=conf)
def split_lines(txt):
    txt = txt.split(',')
    station_id = txt[0]
    wrapper = txt[2]
    temp = float(txt[3]) * 0.1 * (9./5.) + 32.
    return (station_id,wrapper,temp)
lines = sc.textFile('1800.csv')
rdd = lines.map(split_lines)
min_temp = rdd.filter(lambda x: "TMIN" in x[1])
min_temp = min_temp.map(lambda x: (x[0],x[2]))
min_temp = min_temp.reduceByKey(lambda x,y: min(x,y))
res2 = min_temp.collect()
max_temp = rdd.filter(lambda x: "TMAX" in x[1])
max_temp = max_temp.map(lambda x:(x[0],x[2]))
max_temp = max_temp.reduceByKey(lambda x,y: max(x,y))
res = max_temp.collect()
print("Maximum Temperature Per Station:")
for r in res:
    print(r[0],':','{:.2f}F'.format(r[1]))

print('-'*50)
print("Minimum Temperature Per Station:")
for r in res2:
    print(r[0],':','{:.2f}F'.format(r[1]))