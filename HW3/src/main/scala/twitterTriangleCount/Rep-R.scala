package twitterTriangleCount

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RepRMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val MAX_FILTER = 80000
    if (args.length != 2) {
      logger.error("Usage:\ntwitterTriangleCount.RS-R <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Rep-R")
    val sc = new SparkContext(conf)
    val globalTriangleCount = sc.longAccumulator //Global counter defined to keep count of triangle Sum

    val textFile2 = sc.textFile(args(0)) //Input is the edges.csv file

    // Apply MAX_FILTER and map each input row to set of (X,Y)
    val pairRDD2 = textFile2.filter(line => line.split(",")(1).toInt < MAX_FILTER && line.split(",")(0).toInt < MAX_FILTER)
      .map(line => (line.split(",")(0), line.split(",")(1)))

    // For each user now buld the map such that each user is mapped to a set of user it follows
    val broadCastMap =   pairRDD2.map(rddRow => (rddRow._1, Set(rddRow._2))).reduceByKey(_++_)

    println("Size of broadCastMap is: " + broadCastMap)

    //Broad cast the map value
    val smallAsMap = sc.broadcast(broadCastMap.collect().toMap)

    // For each input user Y, check if the value exists in the map
    pairRDD2.foreach(rddRow =>
      if (smallAsMap.value.contains(rddRow._2)) {
        // get list of Z users that Y follows
        var zUsersYFollows: Set[String] = smallAsMap.value.get(rddRow._2).get  // Get all users Z follows
        zUsersYFollows.foreach(zUser =>
          if (smallAsMap.value.contains(zUser)) {
            //Map each user that z follows as X user
            var xUsersZFollow: Set[String] = smallAsMap.value.get(zUser).get
            if (xUsersZFollow.contains(rddRow._1)) {  // If Z follows X
              globalTriangleCount.add(1) //Increment the triangle count by 1 as we validated the path XYZ
            }
          })
      })

    sc.parallelize(Seq(globalTriangleCount.value/3.0)).saveAsTextFile(args(1))
    println("The number of triangles are: " + globalTriangleCount.value/3)
  }
}