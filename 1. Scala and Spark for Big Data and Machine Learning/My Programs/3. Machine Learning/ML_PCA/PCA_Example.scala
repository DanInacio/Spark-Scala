// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors

// SparkSession
val spark = SparkSession.builder().appName("PCA_Example").getOrCreate()

// Create some Data
val data = Array(
  Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
  Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),     // 5 features
  Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)      // 5 features
)

// Perform the operation
val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
val pca = (new PCA()
  .setInputCol("features")
  .setOutputCol("pcaFeatures")
  .setK(3) // Reduce your number of features to 3 principal components
  .fit(df))

// Transform and check out the results
// Check out the results
val pcaDF = pca.transform(df)
val result = pcaDF.select("pcaFeatures") // Each resulting feature is a combination of previous features!
result.show()
