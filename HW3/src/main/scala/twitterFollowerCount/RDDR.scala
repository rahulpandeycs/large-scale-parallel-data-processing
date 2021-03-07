package twitterFollowerCount

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RDDRMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\ntwitterFollowerCount.RDDR <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("RDDR")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val followerCount = textFile
      .map(word => (word.split(",")(1), 1))  // apply split function and map key to 1
      .reduceByKey(_ + _) // Reduce each key value pair (key,1) with key by adding all values per key

    println("ToDebugString output: " + followerCount.toDebugString)
    followerCount.saveAsTextFile(args(1))
  }
}