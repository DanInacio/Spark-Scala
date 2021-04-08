import org.apache.spark.sql.SparkSession

// Start a Spark Session (always when working with Spark DFs)
val spark = SparkSession.builder().getOrCreate()

// Read data file
// val df = spark.read.csv("./Data/CitiGroup2006_2008")                                                   // Everything will be read as a string
// val df = spark.read.option("header","true").csv("./Data/CitiGroup2006_2008")                           // Option will hide the header when printing a number of rows
val df = spark.read.option("header","true").option("inferSchema","true").csv("./Data/CitiGroup2006_2008") // Variables are read with the correct data type

// Print per row (notice everything is read as a string!)
for(row <- df.head(5))
{
  println(row)
}

// Print column names
df.columns

// DF Statistics, same as Python
df.describe().show()

// Select a single column of data and view the data
df.select("Volume").show()
df.select($"Date",$"Close").show()

// Create New column, based on existing ones
df.withColumn("HighPlusLow",df("High") + df("Low")).show()

// Save df in a new object
val df2 = df.withColumn("HighPlusLow",df("High") + df("Low"))

// View Data Types of a dataframe
df2.printSchema()

// Rename columns, select two of them and view
df2.select(df2("HighPlusLow").as("HPL"),df2("Close")).show()
