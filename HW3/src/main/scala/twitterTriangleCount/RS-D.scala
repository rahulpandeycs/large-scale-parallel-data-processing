package twitterTriangleCount

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}

object RSDMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    val MAX_FILTER = 50000  // Define MAX_FILTER

    if (args.length != 2) {
      logger.error("Usage:\ntwitterTriangleCount.RS-R <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("RS-D")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val dataset: Dataset[String] = spark.read.textFile(args(0))  //Read input

    import spark.implicits._
    //Apply MAX_FILTER to allow user id only below the defined constant MAX_FILTER.
    //Also map each row to make them of form (Y,X), so as to get ready for join on Y
    val splitDataset = dataset.filter(line => line.split(",")(1).toInt < MAX_FILTER && line.split(",")(0).toInt < MAX_FILTER)
      .map(line => (line.split(",")(0), line.split(",")(1)))

    //Apply MAX_FILTER to allow user id only below the defined constant MAX_FILTER.
    //Also map each row to make them of form (X,Y), so as to get ready for join on Y
    val splitDataset2 = dataset.filter(line => line.split(",")(1).toInt < MAX_FILTER && line.split(",")(0).toInt < MAX_FILTER)
      .map(line => (line.split(",")(0), line.split(",")(1)))

    //Creating intermediate path
    //Perform a Join on Y, each row now looks like, (Y, (X,Z))
    val joinedDataSet = splitDataset.joinWith(splitDataset2, splitDataset.col("_2") === splitDataset2.col("_1"))

    //Extract XZ path from the dataFrame
    val mapRDDXZ = joinedDataSet.map(joinedDS => ((joinedDS._1._1, joinedDS._2._2), 1))

    val mapRDDZX = dataset.filter(line1 => line1.split(",")(1).toInt < MAX_FILTER && line1.split(",")(0).toInt < MAX_FILTER)
      .map(line1 => ((line1.split(",")(1), line1.split(",")(0)), 1))

    //Counting path2 count for each (X,Z) pair
    val countRDD1 = mapRDDXZ.groupByKey(_._1).reduceGroups((x, y) => ((x._1._1, x._1._2), x._2 + y._2))

    //Counting path2 count for each Edge (Z,X) pair
    val countRDD2 = mapRDDZX.groupByKey(_._1).reduceGroups((x, y) => ((x._1._1, x._1._2), x._2 + y._2))


    // joining tables on common key (X,Z)
    val joinedTriangleCount = countRDD1.joinWith(countRDD2, countRDD1.col("key") === countRDD2.col("key"))

    //Multiplying count for each (X,Z) pair to get
    val triangleCountPerKey = joinedTriangleCount.map(value => value._1._2._2 * value._2._2._2)

    //    //Total triangle Count
    val multiplied = triangleCountPerKey.reduce((x, y) => x + y)/3.0
    sc.parallelize(Seq(multiplied)).saveAsTextFile(args(1))
    println("The number of triangles are: " + multiplied)
  }
}