package twitterTriangleCount

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RSRMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val MAX_FILTER = 40000
    if (args.length != 2) {
      logger.error("Usage:\ntwitterTriangleCount.RS-R <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("RS-R")
    val sc = new SparkContext(conf)

    val textFile1 = sc.textFile(args(0))  //Read edges.csv as texFile1
    val textFile2 = sc.textFile(args(0))   //Read edges.csv as texFile2

    //Apply MAX_FILTER to allow user id only below the defined constant MAX_FILTER.
    //Also map each row to make them of form (Y,X), so as to get ready for join on Y
    val pairRDD1 = textFile1.filter(line => line.split(",")(1).toInt < MAX_FILTER && line.split(",")(0).toInt < MAX_FILTER)
      .map(line => (line.split(",")(1), line.split(",")(0)))

    //Apply MAX_FILTER to allow user id only below the defined constant MAX_FILTER.
    //Also map each row to make them of form (X,Y), so as to get ready for join on Y
    val pairRDD2 = textFile2.filter(line => line.split(",")(1).toInt < MAX_FILTER && line.split(",")(0).toInt < MAX_FILTER)
      .map(line => (line.split(",")(0), line.split(",")(1)))

    //Perform a Join on Y, each row now looks like, (Y, (X,Z))
    val joinedRDD = pairRDD1.join(pairRDD2) //Creating intermediate path

    //First Mapper, to create path XZ
    val mapRDDXZ = joinedRDD.map(joinedRDD=>((joinedRDD._2._1, joinedRDD._2._2),1))

    //First Mapper, to create path ZX, apply MAX filter
    val mapRDDZX = textFile1.filter(line => line.split(",")(1).toInt < MAX_FILTER && line.split(",")(0).toInt < MAX_FILTER)
      .map(line => ((line.split(",")(1),line.split(",")(0)),1))

    //Counting path 2 count for each (X,Z) pair
    val countRDD1 = mapRDDXZ.groupByKey().map(line => ((line._1._1, line._1._2), line._2.size)) //Counting path 2 count for each (X,Z) pair

    ////Counting path 2 count for each (Z,X) pair
    val countRDD2 = mapRDDZX.groupByKey().map(line => ((line._1._1, line._1._2), line._2.size)) //Counting path 2 count for each (Z,X) pair

    val joinedTriangleCount = countRDD1.join(countRDD2)  // joining tables on common key (X,Z)

    val totalTriangleCountPerKey = joinedTriangleCount.mapValues(value =>  value._2*value._1) //Multiplying count for each (X,Z) pair to get

    //Total triangle Count
    val multiplied = totalTriangleCountPerKey.map(_._2).sum()/3.0
    sc.parallelize(Seq(multiplied)).saveAsTextFile(args(1))
    println("The number of triangles are: " + multiplied)
  }
}