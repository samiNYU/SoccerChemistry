import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import spark.implicits._

//File I/otherwise
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val newFilePath = "hdfs:///user/sa5064/Project/Data/FinalData/Clean/"
val tempFilePath = "hdfs:///user/sa5064/Project/Data/TempData/"
if (!fs.exists(new Path(newFilePath))){
  fs.mkdirs(new Path(newFilePath))
} else {
  fs.delete(new Path(newFilePath), true)
  fs.mkdirs(new Path(newFilePath))
}
fs.mkdirs(new Path(tempFilePath))

//hard coded list of possible events
val typeIdList = Array(42,2,3,4,5,6,8,9,10,14,16,17,18,
  19,20,21,22,23,24,25,26,27,28,30,33,34,35,36,37,38,39,40,41,43)

var count = 1
def cleanFile(matchPath :String, lineupPath :String): Unit = {
  //Read Data and the select the columns we want for the analytic
  val thisMatch = spark.read.option("multiLine", true).json(matchPath)
  val rawdf = thisMatch.select("index","type","player","team","possession_team","location","timestamp","shot","substitution")

  //check types
  val checkEvents = rawdf.select("index","type.id").filter(
    !col("id").isInCollection(typeIdList))

  //check players
  val playerIDs = rawdf.select(rawdf("index"),rawdf("player.id").alias("player_id"),rawdf("type.id").alias("type_id")).filter(
    rawdf("type.id") =!= 35 and
      rawdf("type.id") =!= 34 and
      rawdf("type.id") =!= 36 and
      rawdf("type.id") =!= 18 and
      rawdf("type.id") =!= 41 and
      rawdf("type.id") =!= 5
  ).drop("type_id")
  val thisLineUp = spark.read.option("multiLine", true).json(lineupPath)
  val playerList = thisLineUp.select(explode(thisLineUp("lineup.player_id"))).collect().map(r => r.get(0).toString.toLong)
  val checkPlayers = playerIDs.filter(!col("player_id").isInCollection(playerList))

  //check teams
  val teams = rawdf.select("team.id").distinct()
  val isBarca = teams.filter($"id" === 217).count() == 1
  val teamCount = teams.count()

  //check possession teams
  val possession_teams = rawdf.select("possession_team.id").distinct()
  val isBarcaPossession = possession_teams.filter($"id" === 217).count() == 1
  val teamCountPossession = possession_teams.count()

  //check locations
  val invalidX = rawdf.filter(rawdf("location").getItem(0) < -1 or rawdf("location").getItem(0) > 121)
  val invalidY = rawdf.filter(rawdf("location").getItem(1) < -1 or rawdf("location").getItem(1) > 81)

  //check timestamps
  val invalidTimes = rawdf.select("index","timestamp").withColumn(
    "replaced",regexp_replace(col("timestamp"),"""[\d:.]""","")
  ).dropDuplicates("replaced")

  val substitutedf = rawdf.withColumn("substitute",col("substitution.replacement.id")).drop("substitution")
  //check Substitutions
  val subs = substitutedf.select(substitutedf("index"),substitutedf("substitute").alias("substitute_id")).distinct.filter(!$"substitute_id".isNull)
  val checkSubs = subs.filter(!col("substitute_id").isInCollection(playerList))

  //Add columns for Barcelona Goals and Opponent Goals
  val barcaGoal = substitutedf.withColumn(
    "BarcaGoal",
    when(
      (rawdf("shot.outcome.id") === 97 and rawdf("team.id") === 217)
        or (rawdf("type.id")=== 25 and rawdf("team.id") === 217)
      ,1).otherwise(0))

  val otherdf = barcaGoal.withColumn(
    "OtherGoal",
    when(
      (barcaGoal("shot.outcome.id") === 97 and barcaGoal("team.id") =!= 217)
        or (barcaGoal("type.id")=== 25 and barcaGoal("team.id") =!= 217)
      ,1).otherwise(0)).drop("shot")

  //get index for halfTime
  val halftimeIndex1 = otherdf.filter($"type.id" === 18 && $"index">5).agg(min($"index")).head().getLong(0)

  //add a column to indicate half
  //this is so we don't consider events in the different halfs
  val finaldf = otherdf.withColumn("Half",when(col("index")<halftimeIndex1,1).otherwise(2))

  val matchPathArr = matchPath.split("/")
  val match_id = matchPathArr(matchPathArr.size - 1).split(".json")(0)

  //output to user whether unusual results are found or if everything is valid
  println("Cleaning and Profiling match: " + match_id)
  if (checkEvents.count() != 0){
    println("Invalid events")
    checkEvents.show(false)
  } else {
    println("All events are valid.")
  }
  if (checkPlayers.count() != 0){
    println("Invalid players")
    println(playerList.mkString(" "))
    checkPlayers.show(false)
  } else {
    println("All players are valid.")
  }

  if (!isBarca){
    println("FC Barcelona missing from match data")
  }
  if (teamCount != 2){
    println("Team Count is not two")
    teams.show(false)
  }

  if (!isBarcaPossession){
    println("FC Barcelona missing from match data")
  }
  if (teamCountPossession != 2){
    println("Team Count is not two")
    teams.show(false)
  }

  if (invalidX.count() != 0){
    println("Invalid X locations")
    invalidX.show(false)
  } else {
    println("All X locations are valid")
  }
  if (invalidY.count() != 0){
    println("Invalid Y locations:")
    invalidY.show(false)
  } else {
    println("All Y locations are valid")
  }

  if(invalidTimes.count() != 1){
    println("Not all times valid:")
    invalidTimes.show(false)
  } else {
    println("All times are valid")
  }

  if(checkSubs.count() != 0){
    println("Not all substitutes are valid")
    checkSubs.show(false)
  } else {
    println("All substitutes are valid")
  }

  println("Match " + match_id + " done, Count: " + count)
  println("")
  count+=1

  //write data
  finaldf.coalesce(1).write.mode("overwrite").format("parquet").option("header","true").parquet(tempFilePath)
  val tempFileName = fs.globStatus(new Path(tempFilePath+"part*"))(0).getPath.getName
  val newFileName = match_id + ".snappy.parquet"
  fs.rename(new Path(tempFilePath+tempFileName),new Path(newFilePath+newFileName))
}

//clean each match event file
val matchdf = spark.read.parquet("hdfs:///user/sa5064/Project/Data/matchPaths/MatchPaths.snappy.parquet")
matchdf.collect().foreach(line=>cleanFile(line.get(0).toString,line.get(1).toString))
fs.delete(new Path(tempFilePath),true)
