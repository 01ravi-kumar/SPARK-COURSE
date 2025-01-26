from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    # itemID = fields[1]
    amount = round(float(fields[2]),3)

    return (customerID, amount)

input = sc.textFile("./customer-orders.csv")

parsedLines = input.map(parseLine)

totalAmount = parsedLines.reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey()

results = totalAmount.collect()

for result in results:
    print(result)
