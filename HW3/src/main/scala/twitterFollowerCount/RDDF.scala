package twitterFollowerCount

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RDDFMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterFollowerCount.RDD-F <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDDF")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0)) // Read input file
    val followersCount = textFile
      .map(word => (word.split(",")(1), 1)) // apply split function and map key to 1
      .foldByKey(0)(_ + _)  // Reduce each key value pair (key,1) with key by adding all values per key

    println("ToDebugString output: " + followersCount.toDebugString)
    followersCount.saveAsTextFile(args(1))
  }
}