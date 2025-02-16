# Databricks notebook source
# MAGIC %md
# MAGIC # read_csv_data

# COMMAND ----------

flight_df = spark.read.format("csv")\
            .option("header","false")\
            .option("inferschema","false")\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")

flight_df.show(10)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType
schema = StructType([
    StructField('Destination',StringType(),True),
    StructField('Origin',StringType(),True),
    StructField('Count',IntegerType(),True)
])

# flight_df_2 = spark.read.format("csv")\
#             .option("header","false")\
#             .option("inferschema","false")\
#             .schema(schema)\
#             .option("mode","FAILFAST")\
#             .load("/FileStore/tables/2010_summary.csv")

# Above code fails because first line of the file is a hearder which contain the name of the column. 
# If we want to run the code so 
    # either chanege the header option to true which will not read the first line 
    # OR change the mode to PERMISSIVE so the even if there are malformed data, then also let the dataframe create.  
    # OR use skiprow option to skip the n first line of the data  


flight_df_2 = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","false")\
            .schema(schema)\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")

flight_df_2.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling Corrupt records
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

empSchemaWithCorruptRecord = StructType([
    StructField('Id',IntegerType(),True),
    StructField('Name',StringType(),True),
    StructField('Age',IntegerType(),True),
    StructField('Salary',IntegerType(),True),
    StructField('Address',StringType(),True),
    StructField('Nominee',StringType(),True),
    StructField('_corrupt_record',StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC First let's see the different mode in which we can read the records

# COMMAND ----------


emp_df_1 = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","true")\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/emp_data.csv")

emp_df_1.show()

# COMMAND ----------


emp_df_2 = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","true")\
            .option("mode","PERMISSIVE")\
            .load("/FileStore/tables/emp_data.csv")

emp_df_2.show()

# COMMAND ----------


emp_df_3 = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","true")\
            .option("mode","DROPMALFORMED")\
            .load("/FileStore/tables/emp_data.csv")

emp_df_3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write the currupt data into a different column.

# COMMAND ----------


# For this we need to a special column called '_currupt_record' in the schema and its type should be the string type.  

emp_df_4 = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","true")\
            .option("mode","PERMISSIVE")\
            .schema(empSchemaWithCorruptRecord)\
            .load("/FileStore/tables/emp_data.csv")

emp_df_4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Save the currupt data into a location. 

# COMMAND ----------


# For this we need to a special column called '_currupt_record' in the schema and its type should be the string type and we pass one more option called 'badRecordsPath' along with the path where we want to store the records. 
# REMEMBER that we don't specify the mode when we save the records. 

emp_df_5 = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","true")\
            .option('badRecordsPath',"/FileStore/tables/bad_records")\
            .schema(empSchemaWithCorruptRecord)\
            .load("/FileStore/tables/emp_data.csv")

emp_df_5.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bad_records/20250129T165623/bad_records/part-00000-e7ddbfee-0dc2-4266-9b67-cfba8ed64918

# COMMAND ----------

currupt_data = spark.read.json("/FileStore/tables/bad_records/20250129T165623/bad_records/part-00000-e7ddbfee-0dc2-4266-9b67-cfba8ed64918")
currupt_data.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read JSON File

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

# MAGIC %md
# MAGIC line delimited json means that there would be only one record in a single line. Spark is good in reading line delimited json. In multi-line json, spark have to consider the whole document as a object and then it has to find where is the end of the records.  

# COMMAND ----------

line_delimited_json = spark.read.format("json")\
                                .option('inferSchema','true')\
                                .option('mode','PERMISSIVE')\
                                .load("/FileStore/tables/line_delimited.json")
line_delimited_json.show()

# COMMAND ----------

line_delimited_json_with_extra_field = spark.read.format("json")\
                                .option('inferSchema','true')\
                                .option('mode','PERMISSIVE')\
                                .load("/FileStore/tables/single_file.json")
line_delimited_json_with_extra_field.show()

# COMMAND ----------

# MAGIC %md
# MAGIC By default, multiline option is set to default. 

# COMMAND ----------

Multi_line_correct_json = spark.read.format("json")\
                                .option('inferSchema','true')\
                                .option('mode','PERMISSIVE')\
                                .option('multiline','true')\
                                .load("/FileStore/tables/Multi_line_correct.json")
Multi_line_correct_json.show()

# COMMAND ----------

Multi_line_incorrect_json = spark.read.format("json")\
                                .option('inferSchema','true')\
                                .option('mode','PERMISSIVE')\
                                .option('multiline','true')\
                                .load("/FileStore/tables/Multi_line_incorrect.json")
Multi_line_incorrect_json.show()

# COMMAND ----------

incorrect_json = spark.read.format("json")\
                                .option('inferSchema','true')\
                                .option('mode','PERMISSIVE')\
                                .load("/FileStore/tables/corrupted.json")
incorrect_json.show()

# COMMAND ----------

nested_json = spark.read.format("json")\
                                .option('inferSchema','true')\
                                .option('mode','PERMISSIVE')\
                                .option('multiline','true')\
                                .load("/FileStore/tables/nested_json.json")
nested_json.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Dataframe API

# COMMAND ----------

customer = spark.read.format('csv')\
                     .option('header','true')\
                     .option('inferSchema','true')\
                     .load("/FileStore/tables/customer.csv")

customer.show() 

# COMMAND ----------

customer.printSchema()

# COMMAND ----------

customer.write.format('csv')\
              .option('header','true')\
              .option('mode','overwrite')\
              .option("path","/FileStore/tables/write_csv")\
              .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # PartitionBy or BucketBy

# COMMAND ----------

new_customer = spark.read.format('csv')\
                     .option('header','true')\
                     .option('inferSchema','true')\
                     .load("/FileStore/tables/new_customer.csv")

new_customer.show() 

# COMMAND ----------

new_customer.printSchema()

# COMMAND ----------

new_customer.write.format('csv')\
              .option('header','true')\
              .option('mode','overwrite')\
              .partitionBy('address')\
              .option("path","/FileStore/tables/partitionByAddress")\
              .save()

# partitionBy('column_1','column_2',...) if we provides a sequence of columns then data is furthere subdivided into the sub-folders.

# COMMAND ----------

new_customer.write.format('csv')\
              .option('header','true')\
              .option('mode','overwrite')\
              .bucketBy(3,'id')\
              .option("path","/FileStore/tables/bucketById")\
              .saveAsTable('bucketByIdTable')

# .bucketBy(3,'id')\ 3 means divide whole data into 3 bucket based on id column 
# if we use .save() method to save the data, then it will give us error - 'save' does not support bucketBy right now. So we need to save this data as a table which will be stored into the hive store

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataframe Transformations in Spark

# COMMAND ----------

from pyspark.sql import functions as func 

# COMMAND ----------

new_customer.select(func.col('salary')+1).show()

# COMMAND ----------

# multiple ways to select a columns

new_customer.select('id',func.col('name'),new_customer['age'],new_customer.salary).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Alias

# COMMAND ----------

new_customer.select(func.col('id').alias("customer_id")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter or Where
# MAGIC
# MAGIC both are same. We can do same things using both 

# COMMAND ----------

new_customer.filter((func.col('salary')>150000) & (func.col('age')<18)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Literal - It is used to assige the default value to the a culumn or row

# COMMAND ----------

new_customer.select(func.col('id'),func.lit('ab_ra_ka_dabra').alias('last_name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Add a new column using withColumn

# COMMAND ----------

new_customer.withColumn('sur_name',func.lit('KAKE')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Rename a column

# COMMAND ----------

new_customer.withColumnRenamed(existing='id',new='customer_id').show()

# COMMAND ----------

# MAGIC %md
# MAGIC casting the column data type

# COMMAND ----------

new_customer.printSchema()

# COMMAND ----------

new_customer.withColumn('id',func.col('id').cast('string')).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop a column

# COMMAND ----------

new_customer.drop('age').show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Union, UnionAll and UnionByName
# MAGIC
# MAGIC
# MAGIC * Both union and unionAll is same in pyspark dataframe API, But is SQL, union removes the duplicate records and unionAll keeps them.
# MAGIC * Union and UnionAll just stack up the dataframe one upon another, so sequence of column becomes important in that case. This problem is solved by the UnionByName
# MAGIC
# MAGIC

# COMMAND ----------

new_customer_1 = new_customer.filter(func.col('age')<18)
new_customer_2 = new_customer.filter(func.col('age')>=18)

# COMMAND ----------

print(new_customer_1.count())
print(new_customer_2.count())

# COMMAND ----------

new_customer_1.union(new_customer_2).count()

# COMMAND ----------

# MAGIC %md
# MAGIC UnionByName

# COMMAND ----------

new_customer_3 = new_customer.filter(func.col('age')<18).select('name','age','salary')
new_customer_4 = new_customer.filter(func.col('age')>=18).select('salary','age','name')

# COMMAND ----------

new_customer_3.show()

# COMMAND ----------

new_customer_4.show()

# COMMAND ----------

# showing the shortcoming of union or unionAll

new_customer_3.union(new_customer_4).show()

# COMMAND ----------

# to overcome the above problem we use unionByName

new_customer_3.unionByName(new_customer_4).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # CASE and WHEN in Spark
# MAGIC
# MAGIC * We use the CASE and WHEN for if and else in spark for conditional statements 

# COMMAND ----------

new_customer.withColumn("Adult", 
                        func.when(func.col('age')<18, 'No')\
                        .when(func.col('age')>=18, 'Yes')\
                        .otherwise('No_Value')
                        ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Unique and Sorting the records

# COMMAND ----------


data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]

schema = ["Id","Name","Salary","Manager_Id"]

manager_df = spark.createDataFrame(data=data,schema=schema)

manager_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Show only the unique records

# COMMAND ----------

manager_df.distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sort the dataframe

# COMMAND ----------

from pyspark.sql import functions as func
manager_df.sort(func.col('salary')).show()

# COMMAND ----------

manager_df.sort( func.col('Salary').desc() , func.col('Name').desc() ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation in Spark

# COMMAND ----------

# MAGIC %md
# MAGIC Count - It is an action (df.count()) as well as transformation (df.select(count('column')).some_action)
# MAGIC

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

schema = ['Id','Name','Age','Salary','Country','Department']
emp_df = spark.createDataFrame(data=emp_data,schema=schema)
emp_df.show()

# COMMAND ----------

emp_df.count() # it will count null records also in the count value 

# COMMAND ----------

emp_df.select(func.count('*')).show()

# COMMAND ----------

from pyspark.sql import functions as func
emp_df.select(func.count('Name')).show() # it will skip null records in the count value. 

# This is the difference if we run the count function on a specific column. it will only count the non-null value. But if we run the count function in all the columns then it consider the null values also.

# COMMAND ----------

# MAGIC %md
# MAGIC # Group By

# COMMAND ----------

emp_df.groupBy('Country').agg(func.sum('Salary')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Window functions

# COMMAND ----------

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

schema = ['Id','Name','Salary','Department','Gender']
emp_df = spark.createDataFrame(data=emp_data,schema=schema)
emp_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as func

# over here, we are saying that create a window based on the department column i.e. combine all the entries into one window with same department
window = Window.partitionBy("Department")

# over here, we are creating a new column called total_salary and putting the total salary of the department which the person belongs to. This is where the window function is helping us.
  
emp_df.withColumn('total_Salary',func.sum(func.col('Salary')).over(window)).show(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC * Row Number - it gives the ranking based on the order by, even if there are ties between the records then also it will keep giving the rank as incremently
# MAGIC * Rank - It maintain the gaps while giving the rank like if there are 3 person with salary 50K, 50K and 60K, then rank would be 1,1, and 3 respectively. It has skip the 2nd rank. 
# MAGIC * Dense Rank - It does not maintain the gaps while giving the rank like if there are 3 person with salary 50K, 50K and 60K, then rank would be 1,1, and 2 respectively. It does not skip the 2nd rank.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as func

window = Window.partitionBy("Department").orderBy('Salary')
  
emp_df.withColumn('Row_number',func.row_number().over(window))\
    .withColumn('Rank',func.rank().over(window))\
    .withColumn('Dense_rank',func.dense_rank().over(window))\
    .show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC # Lead and Lag in spark
# MAGIC
# MAGIC * Lead - Suppose we have monthly sales data with columns month and sales like sales in Jan, Feb, Mar and so on. If we want to get the <b>next</b> month sales for each row, then we use Lead function.
# MAGIC * Lag - Suppose we have monthly sales data with columns month and sales like sales in Jan, Feb, Mar and so on. If we want to get the <b>previous</b> month sales for each row, then we use Lag function. 
# MAGIC
# MAGIC * Note - These functions works with Window function.

# COMMAND ----------

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

schema = ["Id","Company","Sales_date","Sales"]

product_df = spark.createDataFrame(data=product_data,schema=schema)
product_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as func

window  = Window.partitionBy('Id').orderBy('Sales_date')

last_month_df = product_df.withColumn('Previous_month_sales',func.lag(func.col('Sales'),1,None).over(window))
last_month_df.show()

'''
pyspark.sql.functions.lag(col,offset,default)
Parameters: 
    col:Column or str - name of column or expression
    offset:int, optional default 1 - number of row to extend
    default: optional - default value if no previous record found
'''

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as func

window  = Window.partitionBy('Id').orderBy('Sales_date')

next_month_df = product_df.withColumn('Next_month_sales',func.lead(func.col('Sales'),1,None).over(window))
next_month_df.show()

'''
pyspark.sql.functions.lead(col,offset,default)
Parameters: 
    col:Column or str - name of column or expression
    offset:int, optional default 1 - number of row to extend
    default: optional - default value if no next record found
'''

# COMMAND ----------

# MAGIC %md
# MAGIC # Rows Between in Spark

# COMMAND ----------

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

schema = ["Id","Company","Sales_date","Sales"]

product_df = spark.createDataFrame(data=product_data,schema=schema)
product_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as func

window  = Window.partitionBy('Id').orderBy('Sales_date').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

month_df = product_df.withColumn('first_sales',func.first(func.col('Sales')).over(window))\
                          .withColumn('last_sales',func.last(func.col('Sales')).over(window))
month_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Q - Based on the given emp_df, for every day, find the users on who have worked less then 8 hours. 

# COMMAND ----------

from pyspark.sql import functions as func

emp_data = [(1,"manish","11-07-2023","10:20"),
        (1,"manish","11-07-2023","11:20"),
        (2,"rajesh","11-07-2023","11:20"),
        (1,"manish","11-07-2023","11:50"),
        (2,"rajesh","11-07-2023","13:20"),
        (1,"manish","11-07-2023","19:20"),
        (2,"rajesh","11-07-2023","17:20"),
        (1,"manish","12-07-2023","10:32"),
        (1,"manish","12-07-2023","12:20"),
        (3,"vikash","12-07-2023","09:12"),
        (1,"manish","12-07-2023","16:23"),
        (3,"vikash","12-07-2023","18:08")]

emp_schema = ["Id", "Name", "Date", "Time"]
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

emp_df = emp_df.withColumn("Time",func.from_unixtime(func.unix_timestamp(func.expr(" Concat(date,' ', Time) "),"dd-MM-yyyy HH:mm")))
emp_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as func

window = Window.partitionBy('Name','Date').orderBy('Time').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

short_time_emp_df = emp_df.withColumn('EntryTime',func.first(func.col('Time')).over(window))\
                          .withColumn('ExitTime',func.last(func.col('Time')).over(window))\
                          .withColumn('EntryTime',func.to_timestamp(func.col('EntryTime'),"yyyy-MM-dd HH:mm:ss"))\
                          .withColumn('ExitTime',func.to_timestamp(func.col('ExitTime'),"yyyy-MM-dd HH:mm:ss"))\
                          .withColumn('TotalTime',func.col('ExitTime')-func.col('EntryTime'))

short_time_emp_df.show()

