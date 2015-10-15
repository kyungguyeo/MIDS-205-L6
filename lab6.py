##Lab 6 Code Submissions

crimedata=sc.textFile("Crimes_-_2001_to_present.csv")

noHeaderCrimedata = crimedata.zipWithIndex().filter(lambda (row, index): index > 0).keys()

def remove_header(itr_index, itr): return iter(list(itr)[1:]) if itr_index == 0 else itr

noHeaderCrimedata2 = crimedata.mapPartitionsWithIndex(remove_header)
narcoticsCrimes = noHeaderCrimedata.filter(lambda x: "NARCOTICS" in x)
narcoticsCrimeRecords = narcoticsCrimes.map(lambda r: r.split(","))

narcoticsCrimeTuples = narcoticsCrimes.map(lambda x: (x.split(",")[0], x))

sorted = narcoticsCrimeTuples.sortByKey()

#Submission #1
sorted.take(10)

narcoticsCrimeTuples.take(10)

#The issue with the tuple is that the key is still stored as the first element in the value. This can be fixed by editing the lambda function:

narcoticsCrimeTuples2 = narcoticsCrimes.map(lambda x: (x.split(",")[0], x.split(",")[1:]))

######################################################################

from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)
lines = sc.textFile("weblog_lab.csv")
parts = lines.map(lambda l: l.split('\t'))
Web_Session_Log = parts.map(lambda p: (p[0], p[1], p[2], p[3], p[4]))
schemaString = 'DATETIME USERID SESSIONID PRODUCTID REFERERURL'
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaWebData = sqlContext.createDataFrame(Web_Session_Log, schema)
schemaWebData.registerTempTable('Web_Session_Log')


#Submission #2

results = sqlContext.sql("SELECT count(*) FROM Web_Session_Log WHERE REFERERURL='http://www.ebay.com'")
results.count()

#Submission #3

results.show()





