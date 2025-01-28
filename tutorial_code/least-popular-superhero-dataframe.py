from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperHero").getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(),True)
])


names = spark.read.schema(schema).option('sep'," ").csv("./../ml-100k/Marvel-Names")

lines = spark.read.text("./../ml-100k/Marvel-Graph")

connections = lines.withColumn(
                        'id', 
                        func.split(func.col('value')," ")[0])\
                    .withColumn(
                        'connections', 
                        func.size(func.split(func.col('value')," ")) -1 )\
                    .groupBy('id').agg(func.sum("connections").alias("connections")) 


minConnectionsCount = connections.agg(func.min('connections')).first()[0]

leastPopulars = connections.filter(func.col("connections") == minConnectionsCount)

leastPopularNames = leastPopulars.join(names, "id")


leastPopularNames.select('name').show(leastPopularNames.count())


spark.stop()