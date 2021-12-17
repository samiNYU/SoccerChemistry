import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._

//get list of matches in 2014-2015 FC Barcelona la liga season
//create a dataframe with the list of paths to get to event match data and lineup data
val FS = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val FP = "hdfs:///user/sa5064/Project/Data/matchPaths/"
val seasonMatches = spark.read.option("multiLine", true).json("hdfs:///user/sa5064/Project/Data/open-data/data/matches/11/26.json")
val matchPaths = seasonMatches.select(concat(lit("hdfs:///user/sa5064/Project/Data/open-data/data/events/"),col("match_id"),lit(".json")).alias("match_id"),
  concat(lit("hdfs:///user/sa5064/Project/Data/open-data/data/lineups/"),col("match_id"),lit(".json")).alias("lineup_id"))
matchPaths.write.mode("overwrite").option("header","true").format("parquet").save(FP)
val tempFileName = FS.globStatus(new Path(FP+"part*"))(0).getPath.getName
FS.rename(new Path(FP +tempFileName), new Path(FP + "MatchPaths.snappy.parquet"))
matchPaths.show(false)

