import org.apache.spark.sql.SparkSession
import spark.implicits._ // Enables $scala notation

// Start a Spark Session (always when working with Spark DFs)
val spark = SparkSession.builder().getOrCreate()

// Read file
val df = spark.read.option("header","true").option("inferSchema","true").csv("./Data/CitiGroup2006_2008")
df.printSchema()

// Filter a dataframe
df.filter($"Close" > 480).show() // Scala Notation
df.filter("Close > 480").show()  // SQL Notation
df.filter($"Close" < 480 && $"High" < 480).show() // Multiple filters (scala)
df.filter("Close < 480 AND High < 480").show() // Multiple filters (SQL)

// Collect the results as an array, instead of a dataframe
df.filter("Close < 480 AND High < 480").collect()

// Get number of rows
df.filter("Close < 480 AND High < 480").count() // 397

// Filter for equal value
df.filter($"High" === 484.40).show() // Scala notation requires 3 equal signs in filters!
df.filter("High == 484.40").show()   // SQL Notation is normal (1 or 2 equal signs works!)

// View DF Variable correlation
df.select(corr("High","Low")).show()
// OR
df.select(corr(df("High"),df("Low"))).show()
