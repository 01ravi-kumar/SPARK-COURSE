from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("./Book")

words = inputDF.select(
    func.explode(
        func.split(
            inputDF.value,"\\W+" # By default there would be one column when we read the text file, it is called value. there would also be one row which will contain all the text that is why we are using explode fucntion  
        )
        ).alias('word')
)

wordsWithoutEmptyStrings = words.filter(words.word != "" )

lowercaseWords = wordsWithoutEmptyStrings.select(
                    func.lower(
                        wordsWithoutEmptyStrings.word
                        ).alias('word')
                )

wordCounts = lowercaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort('count')

wordCountsSorted.show(wordCountsSorted.count())