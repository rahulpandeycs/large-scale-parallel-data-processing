package twitterFollowerCount

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DSETMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitterFollowerCount.DSET <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DSET")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val dataset: Dataset[String] = spark.read.textFile(args(0)) // Read input

    import spark.implicits._
    val dataset2 = dataset.map(following => (following.split(",")(1), 1)) // Map each value to 1

    dataset2.printSchema()
    dataset2.show(false)

    val followersCount = dataset2.groupBy("_1").count() //Count number of rows for each key, thats the number of followers

    println("explain() output: " + followersCount.explain(true))
    followersCount.rdd.map(_.toString()).saveAsTextFile(args(1)) // Save to storage

    // Data Frame approach
    //    val textFile = sc.textFile(args(0))
    //
    ////    val textFile2 = spark.read.textFile(args(0))
    //    val pairRDD = textFile
    //      .map(following => (following.split(",")(1), 1))
    //
    ////    pairRDD.toDebugString
    //
    //    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    //    val dataFrame = spark.createDataFrame(pairRDD).toDF("followed", "count")
    ////    val dataset = spark.createDataset(dataFrame)
    //
    //    dataFrame.show()
    //    val followersCount = dataFrame.groupBy("followed").count()
    //    //followersCount.show()
    //    followersCount.rdd.map(_.toString()).saveAsTextFile(args(1))
  }
}