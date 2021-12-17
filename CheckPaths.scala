import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import spark.implicits._
import scala.collection.mutable.ArrayBuffer

//File I/O
var FilePath = "hdfs:///user/sa5064/Project/Data/matchPaths/"
val matchdf = spark.read.parquet(FilePath + "MatchPaths.snappy.parquet")
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
var missing = false
var missingPaths = ArrayBuffer[String]()

//function checks if path exists in hadoop
def pathExist(line :String): Unit = {
  if (!fs.exists(new Path(line))){
    missing = true
    missingPaths += line
  }
}
//check all match paths and lineup paths
matchdf.collect().foreach(line=>pathExist(line.get(0).toString))
if (missing){
  println(missingPaths)
} else {
  println("------------------------------------")
  println("All paths to match event files found")
  println("------------------------------------")
}
matchdf.collect().foreach(line=>pathExist(line.get(1).toString))
if (missing){
  println(missingPaths)
} else {
  println("-------------------------------------")
  println("All paths to match lineup files found")
  println("-------------------------------------")
}

