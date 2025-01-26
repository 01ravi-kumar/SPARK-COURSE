from pyspark.sql import SparkSession, Row


spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(",")
    df_row = Row(
                ID=int(fields[0]),\
                name=str(fields[1].encode("utf-8")),\
                age=int(fields[2]),\
                numFriends=int(fields[3])
            )
    
    return df_row


lines = spark.sparkContext.textFile('./fakefriends.csv')
people = lines.map(mapper)


schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teens = spark.sql(
                    "SELECT * FROM people"
                )


for teen in teens.collect():
    print(teen)

spark.stop()