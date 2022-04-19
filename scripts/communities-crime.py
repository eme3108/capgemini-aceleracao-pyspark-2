from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
def question_1_report(df):
	# Question 1 report;

	df = (df.select('communityname', 'PolicOperBudg')
	        .sort(F.desc('PolicOperBudg')))
	
	df.show(1)

def question_2_report(df):
	# Question 2 report;

	df = (df.select('communityname', 'ViolentCrimesPerPop')
	        .sort(F.desc('ViolentCrimesPerPop')))
	
	df.show(1)

def question_3_report(df):
	# Question 3 report;

	df = (df.select('communityname', 'population')
	        .sort(F.desc('population')))
	
	df.show(1)

def question_4_report(df):
	# Question 4 report;

	df = (df.select('communityname', racepctblack)
	        .sort(F.desc('racepctblack')))
	
	df.show(1)

def question_5_report(df):
	# Question 5 report;

	df = (df.select('communityname', 'pctWWage')
	        .sort(F.desc('pctWWage')))
	
	df.show(1)

def question_6_report(df):
	# Question 6 report;

	df = (df.select('communityname', 'agePct12t29')
	        .sort(F.desc('agePct12t29')))
	
	df.show(1)

def question_7_report(df):
	# Question 7 report;

    df.agg(F.round(F.corr('PolicOperBudg', 'ViolentCrimesPerPop'), 2).alias('Correlation')).show()

def question_8_report(df):
	# Question 8 report;

    df.agg(F.round(F.corr('PctPolicWhite', 'PolicOperBudg'), 2).alias('Correlation')).show()

def question_9_report(df):
	# Question 9 report;

    df.agg(F.round(F.corr('population', 'PolicOperBudg'), 2).alias('Correlation')).show()

def question_10_report(df):
    # Question 10 report;

    df.agg(F.round(F.corr('population', 'ViolentCrimesPerPop'), 2).alias('Correlation')).show()

def question_11_report(df):
    # Question 11 report;

    df.agg(F.round(F.corr('medIncome', 'ViolentCrimesPerPop'), 2).alias('Correlation')).show()


def question_12_report(df):
    # Question 12 report;

    (df.select('communityname', 
			   'racepctblack', 
			   'racePctWhite', 
			   'racePctAsian', 
			   'racePctHisp', 
			   'ViolentCrimesPerPop')
       .sort(F.desc('ViolentCrimesPerPop'))
       .show(10))

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
				   (F.when(F.col('PctPolicWhite').startswith('?'), 0)
				     .otherwise(F.col('PctPolicWhite'))
					 .cast('float').alias('PctPolicWhite')),
				   (F.when(F.col('medIncome').startswith('?'), 0)
				     .otherwise(F.col('medIncome'))
					 .cast('float').alias('medIncome')),
				   (F.when(F.col('racePctWhite').startswith('?'), 0)
				     .otherwise(F.col('racePctWhite'))
					 .cast('float').alias('racePctWhite')),
				   (F.when(F.col('racePctAsian').startswith('?'), 0)
				     .otherwise(F.col('racePctAsian'))
					 .cast('float').alias('racePctAsian')),
				   (F.when(F.col('racePctHisp').startswith('?'), 0)
				     .otherwise(F.col('racePctHisp'))
					 .cast('float').alias('racePctHisp'))
				   )

	question_1_report(df)
	question_2_report(df)
	question_3_report(df)
	question_4_report(df)
	question_5_report(df)
	question_6_report(df)
	question_7_report(df)
	question_8_report(df)
	question_9_report(df)
	question_10_report(df)
	question_11_report(df)
	question_12_report(df)