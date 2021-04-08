import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// Start Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Read data
val data = (spark.read.option("header","true")
                      .option("inferSchema","true")
                      .format("csv")
                      .load(".\\Data\\Clean_USA_Housing.csv"))

data.printSchema()

// Organise data into label and features
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = (data.select(data("Price").as("label"),
                      $"Avg Area Income", $"Avg Area House Age",
                      $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms",
                      $"Area Population"))

df.printSchema()

// Assemble the vector for ML
val assembler = (new VectorAssembler().setInputCols(Array("Avg Area Income",
                                                    "Avg Area House Age",
                                                    "Avg Area Number of Rooms",
                                                    "Avg Area Number of Bedrooms",
                                                    "Area Population"))
                                      .setOutputCol("features"))

val output = assembler.transform(df).select($"label",$"features")

// Train/Test Split 90%/10%
val Array(training,test) = (output.select("label","features")
                                  .randomSplit(Array(0.7,0.3), seed=12345))

// New LR model
val lr = new LinearRegression()

// Parameter Grid Build - extreme values to view the difference
val paramGrid = (new ParamGridBuilder().addGrid(lr.regParam,Array(1000000000,0.001))
                                       .build())

// Train Validation Split (train ratio is how much of this is train/test and how much is left for holdout data)
val trainValidationSplit = (new TrainValidationSplit().setEstimator(lr)
                                                      .setEvaluator(new RegressionEvaluator().setMetricName("r2")) // default (without metricName) is RMSE
                                                      .setEstimatorParamMaps(paramGrid)
                                                      .setTrainRatio(0.8))
// Fit model on training data with the best parameters
val model = trainValidationSplit.fit(training)

// Test model on test data
model.transform(test).select("features","label","prediction").show()

// Evaluate the model metrics
model.validationMetrics // RMSE by default (higher value, the better, even with r2 metric!)

spark.stop()
