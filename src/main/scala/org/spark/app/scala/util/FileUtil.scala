package org.spark.app.scala.util

import org.apache.spark.rdd.RDD
import org.spark.app.spark.SparkCommons.spark

import scala.util.{Failure, Success, Try}

//utility class to save rdd as csv file
object FileUtil {
 def writeToCsvFile (rdd:RDD[(String,String)], output:String) {
   import spark.implicits._
    Try {
      rdd.toDF().write.format("com.databricks.spark.csv")
        .option("header", "false")
        .save(checkOutputFolder(output) + ".csv")
    } match {
      case Success(_) =>  println("processed file output is available at: "+output)
      case Failure(ex) =>  println("ERROR occurred at output writing: "+ex.getMessage)
    }
  }
//if output file name is not provided, use result as default name
  def checkOutputFolder(output:String): String = {
    if(output.endsWith("/"))
      output+"result"
    else
      output
  }

}
