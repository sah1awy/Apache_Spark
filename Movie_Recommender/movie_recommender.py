from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType,LongType,StringType,StructField,StructType,IntegerType
from pyspark.sql import functions as func
import sys

spark = SparkSession.builder.appName("recommeder").master("local[*]").getOrCreate()

#  movieID movieName
# userID movieID rating timestamp
# u.item -> movie_name     u.data -> movies

movieNameSchema = StructType([StructField("movieID",IntegerType(),True),
                             StructField("movieName",StringType(),True)])

movieSchema = StructType([StructField("userID",IntegerType(),True),
                         StructField("movieID",IntegerType(),True),
                         StructField("rating",IntegerType(),True),
                         StructField("timeStamp",LongType(),True)])


def computeCosineSimilarity(data):
    npair = data.withColumn("xx",func.col('rating1') * func.col("rating1")).\
        withColumn("yy",func.col("rating2")*func.col("rating2")).\
        withColumn("xy",func.col("rating1") * func.col('rating2'))
    
    similarity = npair.groupBy("movie1","movie2").\
        agg((func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
             func.sum(func.col("xy")).alias("numerator"),
             func.count(func.col("xy")).alias("numPairs"))
    
    cos_sim = similarity.withColumn("score",func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")).\
                                    otherwise(0)).select("movie1","movie2","score","numPairs")
    
    return cos_sim


def getMovieName(data,movID):
    result = data.filter(func.col("movieID") == movID).select("movieName").collect()[0]
    return result[0]

movieName = spark.read.option("sep","|").schema(schema=movieNameSchema).option("charset", "ISO-8859-1").csv("..\\u.item")

movies = spark.read.option("sep","\t").schema(schema=movieSchema).csv("..\\u.data")

ratings = movies.select("userID","movieID","rating")

moviePairs = movies.alias("ratings1").join(ratings.alias("ratings2"),((func.col("ratings1.userID") == func.col("ratings2.userID")) &\
                                                         (func.col("ratings1.movieID") < func.col("ratings2.movieID")))).\
                                                         select(func.col("ratings1.movieID").alias("movie1"),
                                                                func.col("ratings2.movieID").alias("movie2"),
                                                                func.col("ratings1.rating").alias("rating1"),
                                                                func.col("ratings2.rating").alias("rating2"))


moviePairSimilarity = computeCosineSimilarity(moviePairs).cache()

if len(sys.argv) > 1:
    co = 50
    score = 0.97
    # use it with shell
    # movieID = int(sys.argv[1])
    movieID = 50
    filteredResults = moviePairSimilarity.filter((((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
                                                  (func.col("numPairs") > co) & (func.col("score") > score)))
    
    results = filteredResults.sort(func.col("score").desc()).take(10)
    print("Top 10 similar movies for " + getMovieName(movieName, movieID) + " :")
    for result in results:
        similarMovieID = result.movie1
        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(getMovieName(movieName, similarMovieID) + "\tscore: " \
              + str(round(result.score,2)) + "\tstrength: " + str(result.numPairs))
        

spark.stop()