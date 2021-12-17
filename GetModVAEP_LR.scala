import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.hadoop.fs._
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.sql.expressions.Window
import spark.implicits._

//VectorSlicer to get the probabilites we are interested in
val slicerGoal = new VectorSlicer().setInputCol("Label_GoalProbabilities").setIndices(Array(1)).setOutputCol("Prob_Score")
val slicerConcede = new VectorSlicer().setInputCol("Label_ConcedeProbabilities").setIndices(Array(1)).setOutputCol("Prob_Concede")
val slicerPossession = new VectorSlicer().setInputCol("Label_PossessionProbabilities").setIndices(Array(1)).setOutputCol("Prob_Possess")
val slicerLosePossession = new VectorSlicer().setInputCol("Label_LosePossessionProbabilities").setIndices(Array(1)).setOutputCol("Prob_Lose_Possess")

//convert probabilites column from vector to double
val prob_goal_df = slicerGoal.transform(spark.read.parquet(
  "hdfs:///user/sa5064/Project/Data/FinalData/Predictions/LR/Label_Goal_LRPredictions.snappy.parquet")).select(
  $"index",$"match_id",$"player_id",$"player_name",$"team",
  regexp_replace(
    col("Prob_Score").cast("String"),"\\[|\\]","").cast("Double").alias("Prob_Score")
).cache()

val prob_concede_df = slicerConcede.transform(spark.read.parquet(
  "hdfs:///user/sa5064/Project/Data/FinalData/Predictions/LR/Label_Concede_LRPredictions.snappy.parquet")).select(
  $"index",$"match_id",
  regexp_replace(
    col("Prob_Concede").cast("String"),"\\[|\\]","").cast("Double").alias("Prob_Concede")
).cache()

val prob_possession_df = slicerPossession.transform(spark.read.parquet(
  "hdfs:///user/sa5064/Project/Data/FinalData/Predictions/LR/Label_Possession_LRPredictions.snappy.parquet")).select(
  $"index",$"match_id",
  regexp_replace(
    col("Prob_Possess").cast("String"),"\\[|\\]","").cast("Double").alias("Prob_Possess")
).cache()

val prob_lose_possession_df = slicerLosePossession.transform(spark.read.parquet(
  "hdfs:///user/sa5064/Project/Data/FinalData/Predictions/LR/Label_LosePossession_LRPredictions.snappy.parquet")).select(
  $"index",$"match_id",
  regexp_replace(
    col("Prob_Lose_Possess").cast("String"),"\\[|\\]","").cast("Double").alias("Prob_Lose_Possess")
).cache()

//combine all the columns in the same dataframe in order to perform VAEP calculation
val combined =
  prob_goal_df.join(prob_concede_df,
    Seq("match_id","index"),"inner"
  ).join(prob_possession_df,
    Seq("match_id","index"),"inner"
  ).join(prob_lose_possession_df,
    Seq("match_id","index"),"inner"
  ).cache()

//make sure that each game is analyzed individually so that events in two different games
//don't influence the results
val window = Window.partitionBy("match_id").orderBy("index")

//put the previous probability for each label in a column and drop the top column from each match
val prevs = combined.withColumn("prevProb_Score",
  lag("Prob_Score",1).over(window)
).withColumn("prevProb_Concede",
  lag("Prob_Concede",1).over(window)
).withColumn("prevProb_Possess",
  lag("Prob_Possess",1).over(window)
).withColumn("prevProb_Lose_Possess",
  lag("Prob_Lose_Possess",1).over(window)
).na.drop().cache()

//calculate the difference in probability between events
//this is what captures the "added value"
val diffs = prevs.withColumn("diff_Prob_Score",
  col("Prob_Score")-col("prevProb_Score")
).withColumn("diff_Prob_Concede",
  col("Prob_Concede")-col("prevProb_Concede")
).withColumn("diff_Prob_Possess",
  col("Prob_Possess")-col("prevProb_Possess")
).withColumn("diff_Prob_Lose_Possess",
  col("Prob_Lose_Possess")-col("prevProb_Lose_Possess")
).drop("Prob_Score","prevProb_Score",
  "Prob_Concede","prevProb_Concede",
  "Prob_Possess","prevProb_Possess",
  "Prob_Lose_Possess","prevProb_Lose_Possess").cache()

//compute VAEP score for scoring and for possession
val VAEP = diffs.withColumn("VAEP_Scoring",
  col("diff_Prob_Score")-col("diff_Prob_Concede")
).withColumn("VAEP_Possession",
  col("diff_Prob_Possess")-col("diff_Prob_Lose_Possess")
).drop("diff_Prob_Score","diff_Prob_Concede",
  "diff_Prob_Possess","diff_Prob_Lose_Possess"
).cache()

//add column for metaData in order to see if an interaction is valid
//a valid interaction is consecutive action between different players on same team
val metaData = VAEP.withColumn("prev_player_name",
  lag("player_name",1).over(window)
).withColumn("prev_player_id",
  lag("player_id",1).over(window)
).withColumn("prevTeam",
  lag("team",1).over(window)
).withColumn("prevVAEP_Scoring",
  lag("VAEP_Scoring",1).over(window)
).withColumn("prevVAEP_Possession",
  lag("VAEP_Possession",1).over(window)
).cache()

//add a column to say if it's valid
val isValid = metaData.withColumn("isValidInteraction",
  when($"player_id" =!= $"prev_player_id" and $"team"===217 and $"prevTeam"===217,1).otherwise(0)
)

//calculate the joint modified VAEP score for each joint interaction
val interactionVAEP = isValid.withColumn("interactionVAEP_Scoring",
  (col("VAEP_Scoring")+col("prevVAEP_Scoring"))*col("isValidInteraction")
).withColumn("interactionVAEP_Possession",
  (col("VAEP_Possession")+col("prevVAEP_Possession"))*col("isValidInteraction")
).withColumn("VAEP_Midfield",
  col("interactionVAEP_Scoring")+col("interactionVAEP_Possession")
).drop("VAEP_Scoring","prevVAEP_Scoring",
  "VAEP_Possession","prevVAEP_Possession","interactionVAEP_Possession"
)

//make sure players are in the same order
//for example event with player a to player b would also be summed with
//events from player b to player a
//the order is given by player with the higher ID
def getPlayersOrdering(df: Dataset[Row]) :Dataset[Row] = {
  return df.withColumn("playerPair",
    when($"isValidInteraction"===1 and (col("player_id") > col("prev_player_id")),
      concat(
        lit("("),col("player_name").cast("String"), lit(", "),col("prev_player_name"),lit(")"))
    ).when($"isValidInteraction"===1 and (col("player_id") <= col("prev_player_id")),
      concat(
        lit("("),col("prev_player_name").cast("String"), lit(", "),col("player_name"),lit(")"))
    )
  ).drop("isValidInteraction")
}

//sum the joint interactions across the entire season to get season VAEP scores
val finaldf = getPlayersOrdering(interactionVAEP).groupBy("playerPair").agg(
  sum($"VAEP_Midfield").alias("seasonModVAEP"),sum($"interactionVAEP_Scoring").alias("seasonVAEP")
)

//order by descening order for Modified VAEP
val finaldfordered = finaldf.orderBy(desc("seasonModVAEP"))

//write and print results
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val tempFilePath = "hdfs:///user/sa5064/Project/TempResults/"
val newFilePath = "hdfs:///user/sa5064/Project/Results/LR/"
if (!fs.exists(new Path(newFilePath))){
  fs.mkdirs(new Path(newFilePath))
} else {
  fs.delete(new Path(newFilePath), true)
  fs.mkdirs(new Path(newFilePath))
}
fs.mkdirs(new Path(tempFilePath))

finaldfordered.coalesce(1).write.mode("overwrite").format("parquet").option("header","true").parquet(tempFilePath)
val tempFileName = fs.globStatus(new Path(tempFilePath+"part*"))(0).getPath.getName
fs.rename(new Path(tempFilePath+tempFileName),new Path(newFilePath+"resultsLR.snappy.parquet"))
println("Results in order of new modified VAEP score:")
finaldf.orderBy(desc("seasonModVAEP")).show(false)
println("Results in order of normal VAEP score")
finaldf.orderBy(desc("seasonVAEP")).show(false)
fs.delete(new Path(tempFilePath), true)

