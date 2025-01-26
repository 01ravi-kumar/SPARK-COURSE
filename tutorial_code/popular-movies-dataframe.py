from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, LongType

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

schema = StructType([
    StructField('userID', IntegerType(), True),
    StructField('movieID', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True),
])

# path = "C:\Users\ravi2\Desktop\Spark_Course\ml-100k\u.data"

df = spark.read.option('sep',"\t").schema(schema).csv("./../ml-100k/u.data")

topMovieIDs = df.groupBy('movieID').count().orderBy(func.desc('count'))

topMovieIDs.show()