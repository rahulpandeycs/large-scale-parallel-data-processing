package twitterFollowerCount

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RDDGMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterFollowerCount.RDDG <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD-G")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val groupCount = textFile
      .map(following => (following.split(",")(1), 1))
      .groupByKey()  //groupByKey can cause out of disk problems as data is sent over the network and collected on the reduce workers.
      .mapValues(value => value.sum)

    println("ToDebugString output: " + groupCount.toDebugString)
    groupCount.saveAsTextFile(args(1))
  }
}