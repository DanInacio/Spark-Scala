import org.apache.spark.sql.SparkSession
import spark.implicits._ // Enables $scala notation

// Start a Spark Session (always when working with Spark DFs)
val spark = SparkSession.builder().getOrCreate()

// Read file
val df = spark.read.option("header","true").option("inferSchema","true").csv("./Data/CitiGroup2006_2008")
df.printSchema()
df.show()

// Select month, year, day: returns a column with the date part per row
df.select(month(df("Date"))).show()
df.select(year(df("Date"))).show()

// Average price per year
val df2 = df.withColumn("Year",year(df("Date")))
val dfavgs = df2.groupBy("Year").mean()
dfavgs.select($"Year",$"avg(Close)").show()

// Min price per year
val df3 = df.withColumn("Year",year(df("Date")))
val dfmins = df3.groupBy("Year").min()
dfmins.select($"Year",$"min(Close)").show()
