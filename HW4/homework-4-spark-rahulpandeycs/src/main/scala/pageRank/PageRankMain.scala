package pageRank

import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object PageRankMain {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    showWarning()

    val conf = new SparkConf().setAppName("SparkPageRank")
    val sc = new SparkContext(conf)

    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <value of k> <iter> <output>")
      System.exit(1)
    }


    val k = args(0).toInt
    var start = 1

    val pairV1V2 = new ListBuffer[Row]
    val pairEdgePageRank = new ListBuffer[Row]

    pairEdgePageRank += Row(0, 0.0)

    //Creating Graph and RDD on the fly
    while (start <= k * k) {

      pairEdgePageRank += Row(start, 1.0 / (k * k)) //Defaulting pageRank for each page to 1/k*k

      // Fill pairV1V2 with dummy
      if (start % k == 0) { //Creating the Dummy node at the end of each dangling page
        pairV1V2 += Row(start, 0)
      } else {
        pairV1V2 += Row(start, start + 1) //Connecting pages 1->2->3-4 etc.
      }
      start = start + 1
    }

    //args(2)
    val iters = if (args.length > 1) args(1).toInt else 10 //If number of iteration is not passed default it to 10

    //    val links = lines.map { s =>
    //      val parts = s.split("\\s+")
    //      (parts(0), parts(1))
    //    }.distinct().groupByKey().cache()

    //Convert pageRank and V1V2 pair to RDD
    val pairV1V2RDD = sc.parallelize(pairV1V2) //Converting V1V2 Pair graph to RDD
    val pairEdgePageRankRDD = sc.parallelize(pairEdgePageRank) //Converting pagerank per page list to RDD

    val rddGraph = pairV1V2RDD.map(row => (row(0), List(row(1))))
      .reduceByKey(_++_)
      .cache()  //Cache the graph in the memory

    var rddPageRank = pairEdgePageRankRDD.map(row => (row(0), row(1).toString.toDouble))
      .partitionBy(rddGraph.partitioner.get) //using same partition as rddGraph

    //Page rank for the pages (Not required as already done in previous steps)
    //var ranksTemp = pairEdgePageRank.map(v => 1.0 / (k * k))

    for (i <- 1 to iters) {
      val temp = rddGraph.join(rddPageRank)
      val contribs = rddGraph.join(rddPageRank).flatMap {
        case (vertex, (adjNodes, pageRankOfVertex)) => val totalOutLinks = adjNodes.size
          //Distributing vertex pagerank over its adjacency list
          adjNodes.flatMap(url => List((url, pageRankOfVertex / totalOutLinks), (vertex, 0.0))) //Pass vertex to keep track of originating vertex
      }
      val contribsMain = contribs.reduceByKey(_+_) //Sum pageRanks

      //Get dangling page mass using the lookup for dummy node
      val danglingMass = contribsMain.lookup(0).head

      rddPageRank = contribsMain.map {  //value is sum of pageRank of all pages adj to vertex
        case (vertex, value) => if (vertex.toString.toInt == 0) {
          (vertex, 0.0)
        } else { // Calculating pagerank
          (vertex, 0.15 * (1.0 / (k * k)) + 0.85 * (value + danglingMass * (1.0 / (k * k)))) //Distribute dangling pages mass
        }
      }.sortBy(_._1.toString.toInt)
    }

    //args(2)
    logger.info(rddPageRank.toDebugString)
    rddPageRank.saveAsTextFile(args(2))
    //spark.stop()
  }
}