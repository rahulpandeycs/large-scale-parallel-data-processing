package tw

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object twitterFollowerCountMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\ntw.twitterFollowerCount <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follower Count")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line.split("\r"))
      .map(word => (word.split(",")(1), 1))
      .reduceByKey(_ + _)

    logger.info("The value of count" + counts.toDebugString)
    counts.saveAsTextFile(args(1))
  }
}
