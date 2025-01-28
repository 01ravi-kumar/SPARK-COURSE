from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType
import sys


def computeCosineSimilarity(spark,data):
    pairScore = data\
        .withColumn('xx', func.col('rating1')*func.col('rating1'))\
        .withColumn('yy', func.col('rating2')*func.col('rating2'))\
        .withColumn('xy', func.col('rating1')*func.col('rating2'))

    calculateSimilarity = pairScore\
        .groupBy('movie1','movie2')\
        .agg(
            func.sum(func.col('xy')).alias('numerator'),\
            (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
        )
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result
    

def getMovieName(movieNames,movieID):
    result = movieNames.filter(func.col('movieID' == movieID) \
                              .select('movieTitle').collect()[0]
                              )
    
    return result[0]



spark = SparkSession.builder.appName("MovieSimilarities").master('local[*]').getOrCreate() # here 'local[*]' means that use every core of the machine to run the task

movieNameSchema = StructType([
    StructField('movieID', IntegerType(),True),
    StructField('movieTitle',StringType(),True)
])


movieSchema = StructType([
    StructField('userID', IntegerType(),True),
    StructField('movieID',IntegerType(),True),
    StructField('rating', IntegerType(),True),
    StructField('timestamp',LongType(),True)
])

movieName = spark.read \
    .option('sep',"|")\
    .option('charset',"ISO-8859-1")\
    .schema(movieNameSchema)\
    .csv("./../ml-100k/u.item")


movies = spark.read \
    .option('sep',"\t")\
    .schema(movieSchema)\
    .csv("./../ml-100k/u.data")


ratings = movies.select("userID","movieID",'rating')

moviePairs = movies.alias('rating1')\
            .join(movies.alias('rating2'), (func.col('rating1.userID') == func.col('rating2.userID'))
                  & (func.col('rating1.movieID') < func.col('rating2.movieID')))\
            .select(
                func.col('rating1.movieID').alias('movie1'),
                func.col('rating1.rating').alias('rating1'),
                func.col('rating2.movieID').alias('movie2'),
                func.col('rating2.rating').alias('rating2')
            )


moviePairSimilarity = computeCosineSimilarity(spark,moviePairs).cache()

if len(sys.argv)>1:
    scoreThreshold = 0.97
    coOccurenceThreshold = 50.0

    movieID = int(sys.argv[1])

    filteredResults = moviePairSimilarity.filter(
                            ((func.col('movie1')==movieID) | (func.col('movie1')==movieID)) & \
                            (func.col('score')>scoreThreshold) & (func.col('numPairs')>coOccurenceThreshold) 
                        )
    
    results = filteredResults.sort(func.col('score').desc()).take(10)

    print(" Top 10 sililar movies for "+ getMovieName(movieName,movieID))

    for result in results:
        similarMovieID = result.movie1

        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(getMovieName(movieName,similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: "+ str(result.numPairs))