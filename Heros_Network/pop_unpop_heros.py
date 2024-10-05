# Importing Spark SQL 
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,StructType,StructField
from pyspark.sql import functions as func

# Creating Spark Session
spark = SparkSession.builder.appName("Superheros1").getOrCreate()

# Creating Schema
schema = StructType([StructField("id",IntegerType(),True),
                    StructField("name",StringType(),True)])

# Impoting Data with the specified Schema
names = spark.read.schema(schema=schema).option("sep",' ').csv("..\\Marvel+Names")
lines = spark.read.text("..\\Marvel+Graph")

# Building Connections Data frame that contains ID and Connections Count columns
connections = lines.withColumn("id",func.split(func.col("value"),' ')[0])\
.withColumn("connections",func.size(func.split(func.col("value"),' ')) - 1)\
.groupBy("id").agg(func.sum("connections").alias("connections"))

# Finding the ID of the most Popular Hero
popular = connections.sort(func.col("connections").desc()).first()[0]

# Finding The name of the most Popular Hero
popularname = names.filter(names.id == popular).select("name").first()

connections.filter(func.col("id") == popular).select("connections").collect()[0][0]
print("Most Popular Name is %s with %s co-appearance"%(popularname[0],connections.filter(func.col("id") == popular)\
                                                       .select("connections").collect()[0][0]))

# Finding the ID of the Least Popular Hero
unpopular = connections.sort(func.col("connections")).first()[0]

# Finding The name of the Least Popular Hero
unpopularname = names.filter(names.id == unpopular).select("name").first()
connections.filter(func.col("id") == unpopular).show()

print("Least Popular Name is %s with %s co-appearance"%(unpopularname[0],connections.\
                                                        filter(func.col("id") == unpopular).select("connections").collect()[0][0]))
connections.agg(func.max(func.col("connections"))).collect()