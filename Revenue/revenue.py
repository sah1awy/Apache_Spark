from __future__ import print_function 

from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StandardScaler, VectorAssembler
import numpy as np

spark = SparkSession.builder.appName("LinearRegression").getOrCreate()
lines = spark.sparkContext.textFile("..\\regression.txt")
data = lines.map(lambda x: x.split(',')).map(lambda x: (float(x[0]),Vectors.dense(float(x[1]))))
colNames = ["label","features"]
df = data.toDF(colNames)

scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)
scaler_model = scaler.fit(df)
scaled_df = scaler_model.transform(df)

trainTest = scaled_df.randomSplit([0.7,0.3])
train = trainTest[0]
test = trainTest[1]

lr = LinearRegression(maxIter=10,regParam=0.1,elasticNetParam=0.5)
model = lr.fit(train)

prediction = model.transform(test).cache()
pred = prediction.select("prediction").rdd.map(lambda x:x[0])
label = prediction.select("label").rdd.map(lambda x: x[0])

pred_lab = pred.zip(label).collect()

for x ,y in pred_lab:
    print(round(x,2),'-->',y)

n = len(pred_lab)
err = 0
for x,y in pred_lab:
    err += (y - x)**2

mse = err / n
rmse = np.sqrt(mse)
print("Mean squared Error:",mse)
print("Root Mean Squared Error:",rmse)

mu = np.mean(label.collect())
ssp = np.sum((np.array(label.collect()) - np.array(pred.collect()))**2)
sse = np.sum((np.array(label.collect()) - mu)**2)
r2 = 1 - ssp/sse
print("R2 Score is:",r2)
spark.stop()