import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

// Start a Spark Session
val spark = SparkSession.builder.getOrCreate()

// Load training data
val training = spark.read.format("libsvm").load("C:\\Users\\Daniel\\Desktop\\sample_libsvm_data.txt")

// New LogisticRegression Model
val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

// Fit the Model
val lrModel = lr.fit(training)

// Print some results
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
