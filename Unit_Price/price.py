from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


spark = SparkSession.builder.appName("unitPrice").getOrCreate()
data = spark.read.option("header","true").option("inferSchema","true").csv("..\\realestate.csv")
data.printSchema()
v = VectorAssembler().setInputCols(["No","TransactionDate","HouseAge","DistanceToMRT","NumberConvenienceStores",\
                               "Latitude","Longitude"]).setOutputCol("features")
df = v.transform(data).select("PriceOfUnitArea","features")
df.printSchema()
traintest = df.randomSplit([0.8,0.2])
train = traintest[0]
test = traintest[1]
dt = DecisionTreeRegressor(featuresCol="features",labelCol="PriceOfUnitArea")
model = dt.fit(train)
pred = model.transform(test).cache()
preds = pred.select("prediction").rdd.map(lambda x: x[0])
label = pred.select("PriceOfUnitArea").rdd.map(lambda x: x[0])
res = preds.zip(label).collect()
pred.select("prediction","PriceOfUnitArea","features").show()
print("Predicted  Actual")
for x,y in res:
    print(" {} --> {}".format(round(x,3), y))

evaluator = RegressionEvaluator(labelCol="PriceOfUnitArea", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(pred)
print(f"Root Mean Squared Error (RMSE): {round(rmse,3)}")
spark.stop()