package twitterTriangleCount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.{Dataset, SparkSession}

object RepDMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val MAX_FILTER = 80000  //Global counter defined to keep count of triangle Sum
    if (args.length != 2) {
      logger.error("Usage:\ntwitterTriangleCount.RS-R <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Rep-D")
    val sc = new SparkContext(conf)
    val globalTriangleCount = sc.longAccumulator
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val dataset: Dataset[String] = spark.read.textFile(args(0))

    import spark.implicits._
    // Apply MAX_FILTER and map each input row to set of (X,Y)
    val splitDataset = dataset.filter(line => line.split(",")(1).toInt < MAX_FILTER && line.split(",")(0).toInt < MAX_FILTER)
      .map(line => (line.split(",")(0), line.split(",")(1)))

    // For each user now buld the map such that each user is mapped to a set of user it // follows, _1 and _2 represents follower and followed user
    val groupByKeyDataset = splitDataset.map(datasetRow => (datasetRow._1,Set(datasetRow._2)))
      .groupByKey(_._1)
      .reduceGroups((x,y) => (x._1,x._2 ++ y._2))
      .map(_._2)

    println("Size of broadCastMap is: " + groupByKeyDataset)

    //Broad cast the map value
    val smallAsMap = sc.broadcast(groupByKeyDataset.collect().toMap)

    // For each input user Y, check if the value exists in the map
    splitDataset.foreach(dataSetRow =>
      if (smallAsMap.value.contains(dataSetRow._2)) {
        // get list of Z users that Y follows
        var zUsersYFollows: Set[String] = smallAsMap.value.get(dataSetRow._2).get
        zUsersYFollows.foreach(zUser =>
          if (smallAsMap.value.contains(zUser)) {
            //Map each user that z follows as X user
            var xUsersZFollow: Set[String] = smallAsMap.value.get(zUser).get
            if (xUsersZFollow.contains(dataSetRow._1)) {  // If Z follows X
              globalTriangleCount.add(1) //Increment the triangle count by 1 as we validated the path XYZ
            }
          })
      })

    sc.parallelize(Seq(globalTriangleCount.value/3.0)).saveAsTextFile(args(1)) //Save file to default storage
    println("The count of triangle is: " + globalTriangleCount.value/3)
  }
}