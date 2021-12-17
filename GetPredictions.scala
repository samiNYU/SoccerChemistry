import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.ml.regression.GBTRegressor
import spark.implicits._


//aggregate all the match data together into one dataframe
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val toData = "hdfs:///user/sa5064/Project/Data/FinalData/ReadyForPredictions/"
val files = fs.listStatus(new Path(toData))
val result = files.map(x => spark.read.parquet(x.getPath.toString)).reduce(_.union(_))

//make sure all the data is numeric and labels are doubles
val ready = result.select(
  col("index"),
  col("match_id"),
  col("type.id").alias("type"),
  col("player.id").alias("player_id"),
  col("player.name").alias("player_name"),
  col("team.id").alias("team"),
  col("location")(0).alias("x_location"),
  col("location")(1).alias("y_location"),
  regexp_replace(col("timestamp"),"""[:.]""","").cast("Long").alias("time"),
  col("BarcaGoal"),
  col("OtherGoal"),
  col("Label_Goal").cast("Double"),
  col("Label_Concede").cast("Double"),
  col("Label_Possession").cast("Double"),
  col("Label_LosePossession").cast("Double")).filter(!col("player").isNull).filter(!col("x_location").isNull)

//encode categorical data (turns out not needed for GBT)
val encoder = new OneHotEncoderEstimator().setInputCols(
  Array("type","player_id","team")).setOutputCols(
  Array("typeEncoded","playerEncoded","teamEncoded"))

//assemble all feature columns into one vector column so that it can be passed
//to spark's implementation of classifiation models
val assembler = new VectorAssembler().setInputCols(Array("typeEncoded",
  "playerEncoded",
  "teamEncoded",
  "x_location",
  "y_location",
  "time"
)).setOutputCol("features")

//labels that will be predicted
val toBePredicted = Array("Label_Goal","Label_Concede","Label_Possession","Label_LosePossession")

var newFilePath = "hdfs:///user/sa5064/Project/Data/FinalData/Predictions/GBT/"
val tempFilePath = "hdfs:///user/sa5064/Project/Data/TempData/"
if (!fs.exists(new Path(newFilePath))){
  fs.mkdirs(new Path(newFilePath))
} else {
  fs.delete(new Path(newFilePath), true)
  fs.mkdirs(new Path(newFilePath))
}
fs.mkdirs(new Path(tempFilePath))

//for gradient boosted tree classifier
for (label <- toBePredicted) {
  //create and train new model
  val gbt = new GBTClassifier().setLabelCol(label).setFeaturesCol("features").setProbabilityCol(label+"Probabilities").setMaxIter(10)
  val stagesGBT = Array(encoder,assembler,gbt)
  val pipelineGBT = new Pipeline().setStages(stagesGBT)
  println("REACHED TRAINING STAGE GBT " + label)
  val modelGBT = pipelineGBT.fit(ready)
  println("DONE TRAINING STAGE GBT " + label)
  //save model
  val newGBTModelPath = "hdfs:///user/sa5064/Project/Models/GBT/" + label + "/"
  fs.mkdirs(new Path(newGBTModelPath))
  modelGBT.write.overwrite.save(newGBTModelPath)

  //make predictions
  val predictions = modelGBT.transform(ready)

  //save predictions
  predictions.coalesce(1).write.mode("overwrite").format("parquet").option("header","true").parquet(tempFilePath)
  val tempFileName = fs.globStatus(new Path(tempFilePath+"part*"))(0).getPath.getName
  val newFileName =  label + "_GBTPredictions.snappy.parquet"
  fs.rename(new Path(tempFilePath+tempFileName),new Path(newFilePath+newFileName))

  //update end user
  println(label + " GBT predictions finished")
}

//for logistic regression classifier
newFilePath = "hdfs:///user/sa5064/Project/Data/FinalData/Predictions/LR/"
if (!fs.exists(new Path(newFilePath))){
  fs.mkdirs(new Path(newFilePath))
} else {
  fs.delete(new Path(newFilePath), true)
  fs.mkdirs(new Path(newFilePath))
}
for (label <- toBePredicted) {
  //create and train new model
  val lr = new LogisticRegression().setLabelCol(label).setFeaturesCol("features").setProbabilityCol(label+"Probabilities").setMaxIter(10)
  val stagesLR = Array(encoder,assembler,lr)
  val pipelineLR = new Pipeline().setStages(stagesLR)
  println("REACHED TRAINING STAGE LR " + label)
  val modelLR = pipelineLR.fit(ready)
  println("DONE TRAINING STAGE LR " + label)
  //save model
  val newLRModelPath = "hdfs:///user/sa5064/Project/Models/LR/" + label + "/"
  fs.mkdirs(new Path(newLRModelPath))
  modelLR.write.overwrite.save(newLRModelPath)

  //make predictions
  val predictions = modelLR.transform(ready)

  //save predictions
  predictions.coalesce(1).write.mode("overwrite").format("parquet").option("header","true").parquet(tempFilePath)
  val tempFileName = fs.globStatus(new Path(tempFilePath+"part*"))(0).getPath.getName
  val newFileName =  label + "_LRPredictions.snappy.parquet"
  fs.rename(new Path(tempFilePath+tempFileName),new Path(newFilePath+newFileName))

  //update end user
  println(label + " LR predictions finished")
}
fs.delete(new Path(tempFilePath),true)


