// Start a Spark Session
import org.apache.spark.sql.SparkSession

// Optional: Use the following code below to set the Error reporting
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// Spark Session
val spark = SparkSession.builder().getOrCreate()

// Import clustering Algorithm
import org.apache.spark.ml.clustering.KMeans

// Load data
val dataset = spark.read.format("libsvm").load("C:\\Users\\Daniel\\Desktop\\sample_kmeans_data.txt")

// Trains a k-means model with 2 clusters
val kmeans = new KMeans().setK(2).setSeed(1L)
val model = kmeans.fit(dataset)

// Evaluate clustering by computing Within Set Sum of Squared Errors - the lower, the better
val WSSSE = model.computeCost(dataset)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

// Cluster centers are well separated and error is low
