package org.spark.app.scala.util

import org.spark.app.spark.SparkCommons

import scala.util.Random

/** Utility class to generate input file for program in required format
  * input file and desired output file shall be provided through parameter
  * input: args[0] - any text file
  * output: args[1] -  csv file
   */
object InputFileGenerater {

  def main(args: Array[String]) {

    val spark=SparkCommons.spark

    import spark.implicits._

    val textFile = spark.read.textFile(args(0).trim)
    val counts = textFile.cache().flatMap(line => line.split(" "))
      .map(word => (word, Random.nextInt(20))).filter(w=>(w._1.matches("[A-Za-z]+") && w._1.length > 2))
          .map(w=> w._2-> w._1.toUpperCase)

    val sortedData= counts.sort(counts.columns(0))

   // sortedData.foreach(x=> println(x))

    System.out.println("Total words: " + sortedData.count())

   // counts.rdd.saveAsTextFile(args(1).trim)

    //not an efficient way for extremely large data- coalesce(1)
    sortedData.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(args(1).trim+".csv")
  }
}
