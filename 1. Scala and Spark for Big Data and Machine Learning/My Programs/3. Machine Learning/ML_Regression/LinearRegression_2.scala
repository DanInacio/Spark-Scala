import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import spark.implicits._

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR) // Reduces terminal's reporting when running the code

// Start a Spark session
val spark = SparkSession.builder.getOrCreate()

val data = (spark.read
                .option("header","true")
                .option("inferSchema","true")
                .option("multiline","true")
                .format("csv")
                .load("./Data/Clean_USA_Housing.csv"))

data.printSchema()

// Print one example value from each column separately (data.head(1))
val colnames = data.columns
val firstrow = data.head(1)(0)
println("\n")
println("Example Data Row")
for(ind <- Range(1,colnames.length))
{
  println(colnames(ind))
  println(firstrow(ind))
  println("\n")
}

// ("label","features")
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._

// Parentheses allow for line breaking!
// Do not select address, since it is a string feature!
val df = (data.select(data("Price").as("label"),
          $"Avg Area Income", $"Avg Area House Age",
          $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms",
          $"Area Population"))

// Convert the input features into a Vector
// ML Algorithms in Scala need a vector of features!
val assembler = (new VectorAssembler()
                  .setInputCols(Array("Avg Area Income", "Avg Area House Age",
                                      "Avg Area Number of Rooms", "Avg Area Number of Bedrooms",
                                      "Area Population")).setOutputCol("features"))

// Output with correct formatting for ML Library in Spark!
val output = assembler.transform(df).select($"label",$"features")
output.show()

// Linear Regression Model
val lr = new LinearRegression()

// Fit the model to your training Data
val lrModel = lr.fit(output)

// View Results
val trainingSummary = lrModel.summary
trainingSummary.residuals.show()
trainingSummary.predictions.show()
trainingSummary.r2 // 0.92, not bad, could be better
trainingSummary.rootMeanSquaredError
