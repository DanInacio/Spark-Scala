import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

// Get data (already formatted for ML!)
val data = spark.read.format("libsvm").load("C:\\Users\\Daniel\\Desktop\\sample_linear_regression_data.txt")

// Train/Test SPLIT 90%/10%
val Array(training,test) = data.randomSplit(Array(0.9,0.1), seed=12345)

//data.printSchema()

// New LR model
val lr = new LinearRegression()

// Parameter Grid Build - build a grid of parameters to test the model over
val paramGrid = (new ParamGridBuilder().addGrid(lr.regParam,Array(0.1,0.01))
                                       .addGrid(lr.fitIntercept)
                                       .addGrid(lr.elasticNetParam,Array(0.0,0.5,1.0))
                                       .build())

// Train Validation Split
val trainValidationSplit = (new TrainValidationSplit().setEstimator(lr)
                                                      .setEvaluator(new RegressionEvaluator())
                                                      .setEstimatorParamMaps(paramGrid)
                                                      .setTrainRatio(0.8))
// Fit model on training data with the best parameters
val model = trainValidationSplit.fit(training)

// Test model on test data
model.transform(test).select("features","label","prediction").show()

// model.bestModel -> gets best model parameters
