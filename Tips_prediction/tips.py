from pyspark.sql import SparkSession

pyspark = SparkSession.builder.appName('Practice').getOrCreate()
print(pyspark)
data = pyspark.read.csv('tips.csv',header=True,inferSchema=True)

print(data.show(5))
print(data.printSchema())
print(data.select('total_bill').describe().show())
print(data.columns)

from pyspark.ml.feature import StringIndexer
si = StringIndexer(inputCols=['sex', 'smoker', 'day', 'time'],outputCols=['sex_idx', 'smoker_idx', 'day_idx', 'time_idx'])
df = si.fit(data).transform(data)
print(df.show())

from pyspark.ml.feature import VectorAssembler
vas = VectorAssembler(inputCols=['tip','size','sex_idx','smoker_idx','day_idx','time_idx'],outputCol='Independent Features')
df = vas.transform(df)
df = df.select('Independent Features','total_bill')

from pyspark.ml.regression import LinearRegression
train,test = df.randomSplit([0.75,0.25])
lr = LinearRegression(featuresCol='Independent Features',labelCol='total_bill')
lr = lr.fit(train)
print(lr.coefficients)
print(lr.intercept)
pred = lr.evaluate(test)
print(pred.predictions.show())
print("R2 score: {}\nMean Absolute Error: {}\nMean Squared error: {}".format(pred.r2,pred.meanAbsoluteError,pred.meanSquaredError))
pyspark.stop()
