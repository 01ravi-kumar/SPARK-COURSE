from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}

    with codecs.open("./../ml-100k/u.ITEM", 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames


spark = SparkSession.builder.appName("PopularMovie").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([
    StructField('userID', IntegerType(), True),
    StructField('movieID', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True),
])

df = spark.read.option('sep',"\t").schema(schema).csv("./../ml-100k/u.data")

movieCounts = df.groupBy('movieID').count()

def lookupName(movieID):
    return nameDict.value[movieID]


lookupNameUDF = func.udf(lookupName)


movieWithNames = movieCounts.withColumn('movieTitle',lookupNameUDF(func.col('movieID')))

sortedMovieWithNames = movieWithNames.orderBy(func.desc("count"))

sortedMovieWithNames.show()

spark.stop()