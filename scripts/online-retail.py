from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def question_1_transform(df):
	# Question 1 transformation;

	df = (df.withColumn('StockCode', F.when(F.col('StockCode').contains('gift_0001'), 'gift_0001'))
		    .withColumn('ValueTotal', (F.col('Quantity') * F.col('UnitPrice'))))

	return df

def question_1_report(df):
	# Question 1 report;

	df = question_1_transform(df)
	df = (df.groupBy('StockCode').agg(F.sum('ValueTotal').alias('ValueTotal'))
		    .filter(F.col('StockCode').isNotNull()))

	df.select('StockCode', F.round(F.col('ValueTotal'), 2).alias('ValueTotal')).show()

def question_2_transform(df):
	# Question 2 transformation;

	df = (df.withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/MM/yyyy H:m'))
		    .withColumn('InvoiceMonthDate', F.month(F.col('InvoiceDate'))))

	return df

def question_2_report(df):
	# Question 2 report;
		
	df = question_2_transform(df)
	df = df.select(
		'InvoiceMonthDate',
		(F.round(F.col('Quantity') * F.col('UnitPrice'), 2)).alias('ValueTotal'))
	df = df.groupby('InvoiceMonthDate').agg(F.sum('ValueTotal').alias('ValueTotal'))
	df.show()
 
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

	#question_1_report(df)
	question_2_report(df)