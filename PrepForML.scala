import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.Window

//prepare IO
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val newFilePath = "hdfs:///user/sa5064/Project/Data/FinalData/ReadyForPredictions/"
val tempFilePath = "hdfs:///user/sa5064/Project/Data/TempData/"
if (!fs.exists(new Path(newFilePath))){
  fs.mkdirs(new Path(newFilePath))
} else {
  fs.delete(new Path(newFilePath), true)
  fs.mkdirs(new Path(newFilePath))
}
fs.mkdirs(new Path(tempFilePath))

var count = 1
//this function taks a path, reads the data there and prepares the labels to pass them
//to the ML models
def getScores(path :Path): Unit = {
  val thisMatch = spark.read.parquet(path.toString)
  val matchPathArr = path.toString.split("/")
  val match_id = matchPathArr(matchPathArr.size - 1).split(".snappy")(0)
  //use window in order to separate halfs and to get the next k events
  val k = 5
  val window = Window.partitionBy("Half").rowsBetween(0,k).orderBy("index")

  //label is 1 if there is a goal in next k events
  val goal_df = thisMatch.withColumn("Label_Goal",when(
    (sum("BarcaGoal").over(window)>0 and
      col("possession_team.id") === 217),1).otherwise(0))

  //label is 1 if goal conceding in next k events
  val concede_df = goal_df.withColumn("Label_Concede",when(
    (sum("OtherGoal").over(window)>0 and
      col("possession_team.id") === 217),1).otherwise(0))

  //label 1 if possession is won or kept in next k events
  //-pressure
  //-duel
  //-block
  //-ball recovery
  //-clearance
  //-interception
  val possession_df = concede_df.withColumn("isBarcaDefense",
    when((col("type.id") === 17 and col("team.id") === 217) or //pressure
      (col("type.id") === 6 and col("team.id") === 217) or //block
      (col("type.id") === 4 and col("team.id") === 217) or //duel
      (col("type.id") === 2 and col("team.id") === 217) or //ball recovery
      (col("type.id") === 3 and col("team.id") =!= 217) or //dispossessed
      (col("type.id") === 9 and col("team.id") === 217) or //clearance
      (col("type.id") === 10 and col("team.id") === 217), //interception
      1).otherwise(0)
  ).withColumn("isBarcaPossession",
    when(col("possession_team.id")===217,1).otherwise(0)).withColumn("Label_Possession",
    when(
      sum("isBarcaPossession").over(window)>0 and
        (col("isBarcaDefense")===1 or col("isBarcaPossession")===1)
      ,1).otherwise(0)).drop("isBarcaDefense","isBarcaPossession")


  //label is 1 if possession is lost or unable to be won in next k events
  val dipossession_df = possession_df.withColumn("isOtherDefense",
    when((col("type.id") === 17 and col("team.id") =!= 217) or //pressure
      (col("type.id") === 6 and col("team.id") =!= 217) or //block
      (col("type.id") === 4 and col("team.id") =!= 217) or //duel
      (col("type.id") === 2 and col("team.id") =!= 217) or //ball recovery
      (col("type.id") === 3 and col("team.id") === 217) or //dispossessed
      (col("type.id") === 9 and col("team.id") =!= 217) or //clearance
      (col("type.id") === 10 and col("team.id") =!= 217), //interception
      1).otherwise(0)
  ).withColumn("isOtherPossession",
    when(col("possession_team.id")=!=217,1).otherwise(0)).withColumn("Label_LosePossession",
    when(
      sum("isOtherPossession").over(window)>0 and col("isOtherDefense")===1
      ,1).otherwise(0)).drop("isOtherPossession","isOtherDefense")

  //add a literal column with the match id so that final VAEP score calculation
  //does not take into events from different games
  val finaldf = dipossession_df.withColumn("match_id",lit(match_id))
  finaldf.coalesce(1).write.mode("overwrite").format("parquet").option("header","true").parquet(tempFilePath)

  //write data
  val tempFileName = fs.globStatus(new Path(tempFilePath+"part*"))(0).getPath.getName
  val newFileName = match_id + "PREDICT.snappy.parquet"
  fs.rename(new Path(tempFilePath+tempFileName),new Path(newFilePath+newFileName))

  println("Match " + match_id + " ready for Predictions, Count: " + count)
  println("")
  count+=1
}

//call the function above on each file
val toData = "hdfs:///user/sa5064/Project/Data/FinalData/Clean/"
val files = fs.listStatus(new Path(toData))
files.foreach(x => getScores(x.getPath))
fs.delete(new Path(tempFilePath),true)
