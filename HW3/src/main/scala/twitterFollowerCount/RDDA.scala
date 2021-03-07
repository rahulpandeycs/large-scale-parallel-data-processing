package twitterFollowerCount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDAMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterFollowerCount.RDD-A <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDDA")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))                        // Read input file
    val followersPair = textFile
      .map(following => (following.split(",")(1), 1))  // apply split function and map key to 1

    val initialCount = 0 //Starting count of triangle = 0
    val addCounts =  (n:Int, v: Int) => n+1  // For each v in input, add 1 to previous accumulated value

    //Define aggregate function to add values across partition.
    val sumAcrossPartitions =  (n1: Int, n2: Int) => n1+n2

    //Count followers per key, using aggregateByKey, passing the initial count = 0,
    // passing the above defined aggregate function.
    val followersCount =  followersPair.aggregateByKey(initialCount)(addCounts,sumAcrossPartitions)

    println("ToDebugString output: " + followersCount.toDebugString)
    followersCount.saveAsTextFile(args(1)) //Save file to default storage
  }
}