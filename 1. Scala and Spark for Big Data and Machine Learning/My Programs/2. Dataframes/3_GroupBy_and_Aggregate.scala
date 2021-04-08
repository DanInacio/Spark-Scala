import org.apache.spark.sql.SparkSession
import spark.implicits._ // Enables $scala notation

// Start a Spark Session (always when working with Spark DFs)
val spark = SparkSession.builder().getOrCreate()

// Read file
val df = spark.read.option("header","true").option("inferSchema","true").csv("./Data/Sales.csv")
df.printSchema()
df.show()

// GROUP BY and AGGREGATE
df.groupBy("Company").mean().show()
df.groupBy("Company").count().show()
df.groupBy("Company").max().show()
df.groupBy("Company").min().show()
df.groupBy("Company").sum().show()

// Other Aggregate Functions
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
df.select(countDistinct("Sales")).show() //approxCountDistinct
df.select(sumDistinct("Sales")).show()
df.select(variance("Sales")).show()
df.select(stddev("Sales")).show()
df.select(collect_set("Sales")).show() // only the unique values of Sales column

// ORDER BY
df.show()
df.orderBy("Sales").show() // ASC is default
df.orderBy($"Sales".desc).show()
