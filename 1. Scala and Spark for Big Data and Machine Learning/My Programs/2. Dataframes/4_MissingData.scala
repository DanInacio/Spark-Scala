import org.apache.spark.sql.SparkSession
import spark.implicits._ // Enables $scala notation

// Start a Spark Session (always when working with Spark DFs)
val spark = SparkSession.builder().getOrCreate()

// Read file
val df = spark.read.option("header","true").option("inferSchema","true").csv("./Data/ContainsNull.csv")
df.printSchema()
df.show()

// DROP: drops all rows with even a single null, or less than a minimum number of NON-NULL values
df.na.drop().show()

df.show()
df.na.drop(2).show() // Drop rows with less than 2 actual values

// FILL
df.show()
df.na.fill(100).show() // Fills number columns' NA values with the specified value
df.na.fill("Missing Name").show() // Fills string columns' NA values with the specified value

// CORRECT WAY TO DO THIS
df.na.fill("New Name",Array("Name")).show() // Specify the columns you are handling NAs at
df.na.fill(200,Array("Sales")).show()
