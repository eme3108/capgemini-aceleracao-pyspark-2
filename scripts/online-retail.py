from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType([
		StructField("InvoiceNo",  	IntegerType(),  True),
		StructField("StockCode", 	StringType(),  	True),
		StructField("Description",  StringType(),   True),
		StructField("Quantity",  	IntegerType(),  True),
		StructField("InvoiceDate",  StringType(), 	True),
		StructField("UnitPrice",   	StringType(), 	True),
		StructField("CustomerID",  	IntegerType(),  True),
		StructField("Country",  	StringType(),  	True)
	])

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

	# Transformation UnitPrice e InvoiceDate
	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
		   .withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/MM/yyyy H:m')))