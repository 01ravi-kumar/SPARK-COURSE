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

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col('id') == mostPopular[0]).select('name').first() # mostPopular[0] here means take only 1st column and from this dataframe take the 'name' column 

print(mostPopularName[0] + " is the most popular superhero with "+ str(mostPopular[1])+ " co-actors")


spark.stop()