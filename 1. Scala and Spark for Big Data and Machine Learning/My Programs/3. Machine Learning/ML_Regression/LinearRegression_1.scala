// Example taken from LinearRegressionExample in Spark examples folder
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

def main(): Unit = {
  // Create Session
    val spark = SparkSession
      .builder
      .appName("LinearRegressionExample")
      .getOrCreate()

  // Path to File
  val path = "C:\\Users\\Daniel\\Desktop\\sample_linear_regression_data.txt"

  // Load training data
  val training = spark.read.format("libsvm").load(path)
  training.printSchema()

  // Create LinearRegression Object
  val lr = new LinearRegression()
    .setMaxIter(100)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  // Fit the model
  val lrModel = lr.fit(training)

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  // Summarize the model over the training set and print out some metrics
  val trainingSummary = lrModel.summary
   println(s"numIterations: ${trainingSummary.totalIterations}")
   println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
   trainingSummary.residuals.show() // Residuals are the differences between real and predicted values
   println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
   println(s"r2: ${trainingSummary.r2}") // 0.022 is VERY POOR!

   spark.stop()
}
main()
