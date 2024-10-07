from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


spark = SparkSession.builder.appName('Regression').getOrCreate()
data = spark.read.csv('test1.csv',header=True,inferSchema=True)
data.show()
data.describe().show()
data.printSchema()

data.columns
data.groupby('Name').count().show()
data = data.withColumnRenamed('age','Age')
vec = VectorAssembler(inputCols=['Age','Experience'],outputCol='Independent_feat')
dc = vec.transform(data)
dc = dc.select(['Independent_feat','Salary'])
dc.show()
lr = LinearRegression(featuresCol='Independent_feat',labelCol='Salary')
train,test = dc.randomSplit([0.75,0.25])
regressor = lr.fit(train)
print(regressor.coefficients)
print(regressor.intercept)
pred = regressor.evaluate(test)
pred.predictions.show()
print(pred.meanAbsoluteError)
print(pred.meanSquaredError)
spark.stop()