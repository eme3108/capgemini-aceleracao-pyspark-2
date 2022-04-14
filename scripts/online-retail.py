from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def question_1_transform(df):
	# Question 1 transformation;

	df = (df.withColumn('StockCode', F.when(F.col('StockCode').contains('gift_0001'), 'gift_0001'))
		    .withColumn('ValueTotal', (F.col('Quantity') * F.col('UnitPrice')))
			.filter(~F.col('InvoiceNo').startswith('c'))
			.filter(~F.col('StockCode').startswith('PADS')))

	return df

def question_1_report(df):
	# Question 1 report;

	df = question_1_transform(df)
	df = (df.groupBy('StockCode').agg(F.sum('ValueTotal').alias('ValueTotal'))
		    .filter((F.col('StockCode').isNotNull())))
	df.select('StockCode', 
			   F.round(F.col('ValueTotal'), 2).alias('ValueTotal')).show()

def question_2_transform(df):
	# Question 2 transformation;

	df = (df.withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/MM/yyyy H:m'))
		    .withColumn('InvoiceMonthDate', F.month(F.col('InvoiceDate')))
			.filter(~F.col('InvoiceNo').startswith('c'))
			.filter(~F.col('StockCode').startswith('PADS')))

	return df

def question_2_report(df):
	# Question 2 report;
		
	df = question_2_transform(df)
	df = df.select(
		'InvoiceMonthDate',
		(F.round(F.col('Quantity') * F.col('UnitPrice'), 2)).alias('ValueTotal'))
	df = df.groupby('InvoiceMonthDate').agg(F.sum('ValueTotal').alias('ValueTotal'))
	df.show()

def question_3_transform(df):
	# Question 3 transformation;
	df = (df.withColumn('StockCode', F.when(F.col('StockCode').startswith('S'), 'S'))
			.filter(~F.col('InvoiceNo').startswith('c'))
			.filter(~F.col('StockCode').startswith('PADS')))
	df = (df.select(
				'StockCode', 
				(F.col('Quantity') * F.col('UnitPrice')).alias('ValueTotal'))
			.filter(F.col('StockCode') == 'S')
			.groupby('StockCode').agg(F.sum(F.col('ValueTotal')).alias('ValueTotal')))
	
	return df

def question_3_report(df):
	# Question 3 report;

	df = question_3_transformation(df)
	df.select('StockCode', F.round(F.col('ValueTotal'), 2).alias('ValueTotal')).show()
 
def question_4_report(df):
	# Question 4 report

	df = (df.groupBy('Description').agg(F.sum('Quantity').alias('Quantity'))
			.filter(~F.col('InvoiceNo').startswith('c'))
			.filter(~F.col('StockCode').startswith('PADS')))

	df.sort(F.desc(F.col('Quantity'))).show(1)

def question_6_transform(df):
	# Question 6 transformation;

	df = (df.withColumn('InvoiceDateHour', F.hour(F.col('InvoiceDate')))
			.filter(~F.col('InvoiceNo').startswith('c'))
			.filter(~F.col('StockCode').startswith('PADS')))

	return df

def question_6_report(df):
	# Question 6 report;

	df = question_6_transform(df)
	df = (df.select('InvoiceDateHour', 'Quantity')
	        .groupBy('InvoiceDateHour').agg(F.sum('Quantity').alias('TotalQuantity'))
		    .sort(F.desc(F.col('TotalQuantity'))))
	df.show()

def question_7_transform(df):
	# Question 7 transformation;

	df = (df.withColumn('InvoiceDateMonth', F.month(F.col('InvoiceDate')))
			.filter(~F.col('InvoiceNo').startswith('c'))
			.filter(~F.col('StockCode').startswith('PADS')))

	return df

def question_7_report(df):
	# Question 7 report;

	df = question_7_transform(df)
	df = (df.groupBy('InvoiceDateMonth').agg(F.sum('Quantity').alias('TotalQuantity'))
		   .sort(F.desc(F.col('TotalQuantity'))))
	df.show(1)

def question_9_transform(df):
	# Question 9 transform;

	df = (df.select('InvoiceNo', 'Country', (F.col('Quantity') * F.col('UnitPrice')).alias('ValueTotal'))
			.filter(~F.col('InvoiceNo').startswith('c'))
			.filter(~F.col('StockCode').startswith('PADS')))

	return df

def question_9_report(df):
	# Question 9 report;

	df = question_9_transform(df)

	df = (df.groupby('Country').agg(F.sum(F.col('ValueTotal')).alias('ValueTotal'))
			.sort(F.desc(F.col('ValueTotal'))))

	df.select('Country', F.round(F.col('ValueTotal'), 2).alias('ValueTotal')).show(1)

def question_10_transform(df):
	# Question 10 transform;

	df = (df.select('InvoiceNo', 'StockCode', 'Country', (F.col('Quantity') * F.col('UnitPrice')).alias('ValueTotal'))
		   .filter(F.col('ValueTotal') > 0)
		   .filter(~F.col('InvoiceNo').startswith('c'))
		   .filter(~F.col('StockCode').startswith('PADS')))

	return df

def question_10_report(df):
	# Question 10 report;

	df = question_10_transform(df)

	df = (df.groupBy('StockCode', 'Country')
	   .agg(F.sum(F.col('ValueTotal')).alias('ValueTotal'))
	   .filter(F.col('StockCode') == 'M'))

	df.select('StockCode', 'Country', F.round(F.col('ValueTotal'), 2).alias('ValueTotal')).show(1)

def question_11_transform(df):
	# Question 11 transform;

	df = (df.select('InvoiceNo',
					(F.col('Quantity') * F.col('UnitPrice')).alias('ValueTotal'))
		   .filter(F.col('ValueTotal') > 0)
		   .filter(~F.col('InvoiceNo').startswith('c'))
		   .filter(~F.col('StockCode').startswith('PADS')))


	return df

def question_11_report(df):
	# Question 11 report;

	df = question_11_transform(df)

	(df.groupby('InvoiceNo').agg(F.round(F.sum(F.col('ValueTotal')), 2).alias('ValueTotal'))
	   .filter(F.col('CustomerID').isNotNull())
	   .filter(~F.col('StockCode').startswith('PADS'))
	   .sort(F.desc(F.col('ValueTotal'))).show(1))

def question_12_report(df):
	# Question 12 report;

	(df.groupby('InvoiceNo').agg(F.sum(F.col('Quantity')).alias('Quantity'))
	   .filter(~F.col('InvoiceNo').startswith('c'))
	   .filter(~F.col('StockCode').startswith('PADS'))
	   .sort(F.desc(F.col('Quantity'))).show(1))

def question_13_report(df):
	# Question 13 report;

	(df.groupby('CustomerID').agg(F.count(F.col('InvoiceNo')).alias('InvoiceNoFrequency'))
	   .filter(F.col('CustomerID').isNotNull())
	   .filter(~F.col('StockCode').startswith('PADS'))
	   .sort(F.desc(F.col('InvoiceNoFrequency'))).show(1))

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
	#question_2_report(df)
	#question_3_report(df)
	#question_4_report(df)
	#question_6_report(df)
	#question_7_report(df)
	#question_9_report(df)
	#question_10_report(df)
	#question_11_report(df)
	#question_12_report(df)
	#question_13_report(df)