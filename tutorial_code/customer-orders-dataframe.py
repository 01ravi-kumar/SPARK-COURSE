from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType


spark = SparkSession.builder.appName("Customer-Orders").getOrCreate()

schema = StructType([
    StructField('customerID',IntegerType(),True),
    StructField('OrderID',IntegerType(),True),
    StructField('expense', FloatType(), True)
])


df = spark.read.schema(schema).csv('./customer-orders.csv')
df = df.select('customerID','expense')


totalExpense = df.groupBy('customerID').agg(func.sum('expense').alias('expense'))

totalExpenseSort = totalExpense.select('customerID', func.round(totalExpense.expense,2).alias('expense')).sort('expense')

totalExpenseSort.show(totalExpenseSort.count())

# totalExpense.Sort = totalExpense.sort('')




spark.stop()