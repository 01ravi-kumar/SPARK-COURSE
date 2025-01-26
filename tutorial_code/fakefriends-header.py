from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option('header',"true").option('inferSchema','true').csv('./fakefriends-header.csv')


friendsByAge = people.select('age','friends')

# use the inbuild method to solve the problem 
print("IN-BUILD")
friendsByAge = people.groupBy('age').avg("friends").sort('age')
friendsByAge.show()

# usign the 'functions' utility of pyspark
print("FUNCTION UTILITY")
friendsByAge = people.groupBy('age').agg(func.round(func.avg("friends"),2).alias('friends_avg')).sort('age')
friendsByAge.show()


spark.stop()