from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
def question_1_report(df):
	# Question 1 report;

	df = (df.groupBy('communityname').agg(F.round(F.sum('PolicOperBudg'), 2).alias('PolicOperBudg'))
	        .sort(F.desc('PolicOperBudg')))
	
	df.show(1)

def question_2_report(df):
	# Question 2 report;

	df = (df.groupBy('communityname')
	        .agg(F.round(F.sum('ViolentCrimesPerPop'), 2).alias('ViolentCrimesPerPop'))
	        .sort(F.desc('ViolentCrimesPerPop')))
	
	df.show(1)

def question_3_report(df):
	# Question 3 report;

	df = (df.groupBy('communityname')
	        .agg(F.round(F.sum('population'), 2).alias('population'))
	        .sort(F.desc('population')))
	
	df.show(1)

def question_4_report(df):
	# Question 4 report;

	df = (df.groupBy('communityname')
	        .agg(F.round(F.sum('racepctblack'), 2).alias('racepctblack'))
	        .sort(F.desc('racepctblack')))
	
	df.show(1)

def question_5_report(df):
	# Question 5 report;

	df = (df.groupBy('communityname')
	        .agg(F.round(F.sum('pctWWage'), 2).alias('pctWWage'))
	        .sort(F.desc('pctWWage')))
	
	df.show(1)

def question_6_report(df):
	# Question 6 report;

	df = (df.groupBy('communityname')
	        .agg(F.round(F.sum('agePct12t29'), 2).alias('agePct12t29'))
	        .sort(F.desc('agePct12t29')))
	
	df.show(1)

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))

	# Definição e tratamento dos campos
	df = df.select((F.when(F.col('community').startswith('?'), 0)
					 .otherwise(F.col('community'))
					 .cast('integer')
					 .alias('community')),
				   'communityname',
				   (F.when(F.col('PolicOperBudg').startswith('?'), 0)
					 .otherwise(F.col('PolicOperBudg'))
					 .cast('float').alias('PolicOperBudg')),
				   (F.when(F.col('PolicBudgPerPop').startswith('?'), 0)
				     .otherwise(F.col('PolicBudgPerPop'))
					 .cast('float').alias('PolicBudgPerPop')),
				   F.col('ViolentCrimesPerPop').cast('float'),
				   F.col('population').cast('float'),
				   F.col('racepctblack').cast('float'),
				   F.col('pctWWage').cast('float'),
				   (F.when(F.col('agePct12t29').startswith('?'), 0)
				     .otherwise(F.col('agePct12t29'))
					 .cast('float').alias('agePct12t29')),
				   )

	question_1_report(df)
	question_2_report(df)
	question_3_report(df)
	question_4_report(df)
	question_5_report(df)
	question_6_report(df)