// https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html#year(e:org.apache.spark.sql.Column):org.apache.spark.sql.Column

// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete the commented tasks below!

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
import spark.implicits._ // Enables $scala notation

val spark = SparkSession.builder().getOrCreate()

// Load the Netflix Stock CSV File, have Spark infer the data types.
val df = spark.read.option("header","true").option("inferSchema","true").csv("./Data/Netflix_2011_2016.csv")

// What are the column names?
df.columns

// What does the Schema look like?
df.printSchema()

// Print out the first 5 rows.
df.head(5)

// Use describe() to learn about the DataFrame.
df.describe().show()

// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
val df2 = df.withColumn("HVRatio",df("High") * df("Volume"))
df2.show()

// What day had the Peak High in Price?
df2.orderBy($"High".desc).show(1)

// What is the mean of the Close column?
df.select(mean("Close")).show()

// What is the max and min of the Volume column?
df.select(min("Volume")).show()
df.select(max("Volume")).show()

// For Scala/Spark $ Syntax

// How many days was the Close lower than $ 600?
df.filter("Close < 600").count() // 1218 days

// What percentage of the time was the High greater than $500 ?
df.filter("High > 500").count().toDouble / df.count().toDouble * 100 // 4.9%

// What is the Pearson correlation between High and Volume?
df.select(corr("High","Volume")).show()

// What is the max High per year?
val df3 = df2.withColumn("Year",year(df("Date")))
val dfmaxes = df3.groupBy("Year").max()
dfmaxes.select($"Year",$"max(High)").show()

// What is the average Close for each Calender Month?
val df4 = df2.withColumn("Month",month(df("Date")))
val dfavgs = df4.groupBy("Month").mean()
dfavgs.select($"Month",$"avg(Close)").show()
