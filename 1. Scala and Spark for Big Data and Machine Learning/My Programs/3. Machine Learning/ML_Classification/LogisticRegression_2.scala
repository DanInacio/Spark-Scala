import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// Start a Spark Session
val spark = SparkSession.builder.getOrCreate()

// Load data
val data = (spark.read.option("header","true")
                      .option("inferSchema","true")
                      .format("csv")
                      .load("./Data/titanic.csv"))

// Print the Schema
data.printSchema()

// Print 1 row of Data
data.head(1)

// Set up the dataframe with the format for Machine Learning
val logregDataAll = (data.select(data("Survived").as("label"),
                      $"Pclass", $"Name", $"Sex", $"Age", $"SibSp",
                      $"Parch", $"Fare", $"Embarked"))

// Drop missing values
val logregData = logregDataAll.na.drop()

// Convert categorical data into dummy variables (ONE-HOT ENCODING, like in Python pandas.get_dummies() )
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors

val genderIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")         // String to numerical
val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkIndex") // String to numerical

val genderEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVector")       // Numerical to One Hot Encoding as 0 or 1
val embarkEncoder = new OneHotEncoder().setInputCol("EmbarkIndex").setOutputCol("EmbarkVector") // Numerical to One Hot Encoding as 0 or 1

// Assemble label and features into a vector for ML
// (label,features)
val assembler = (new VectorAssembler()
                     .setInputCols(Array("Pclass", "SexVector", "Age", "SibSp",
                                         "Parch", "Fare", "EmbarkVector"))
                     .setOutputCol("features")) // No name column! We won't get anything out of it in the model...

// Split data into training and test set
val Array(training,test) = logregData.randomSplit(Array(0.7,0.3),seed=12345) // Seed to ensure split is always equal

// Insert all Indexers, Encoders and Assemblers into a PIPELINE
import org.apache.spark.ml.Pipeline

// New LogisticRegression Model
val lr = new LogisticRegression()

// New pipeline - all stages setup and we can treat it as the full ML model
val pipeline = new Pipeline().setStages(Array(genderIndexer,embarkIndexer,genderEncoder,embarkEncoder,assembler,lr))

// Fit the Model to training data
val model = pipeline.fit(training)

// Save results of test data
val results = model.transform(test)

//////////////////////
// MODEL EVALUATION //
//////////////////////
import org.apache.spark.mllib.evaluation.MulticlassMetrics

// If this is now in Dataframe ML library, the command will be different!
val predictionAndLabels = results.select($"prediction",$"label").as[(Double,Double)].rdd

val metrics = new MulticlassMetrics(predictionAndLabels)

println("Confusion Matrix:")
println(metrics.confusionMatrix)
// 114.0 16.0
// 27.0  61.0

// DO NOT USE metrics.ACCURACY, metrics.RECALL OR metrics.PRECISION
// THEY DO NOT WORK!
