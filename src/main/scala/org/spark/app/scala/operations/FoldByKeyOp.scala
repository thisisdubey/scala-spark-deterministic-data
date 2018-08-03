package org.spark.app.scala.operations

import org.apache.spark.rdd.RDD
import org.spark.app.spark.SparkCommons.spark

/**
  * object which operates on input filename
  * using reduce by key to get the total count against each key
  * then fold by key to accumulate data (color, count) for each key
  * and pick the largest one
  * in case of same (color, count)- pick the first one encountered
  */
object FoldByKeyOp {

  def operate(fileName:String): RDD[(String,String)] = {
    val input= spark.read.format("com.databricks.spark.csv").option("header", "false").load(fileName)

    input.rdd.cache().map(w=>(w,1)).reduceByKey(_+_)
      .map(w=>(w._1(0),(w._1(1),w._2)))
      .foldByKey(("dummy", 0))((acc,count)=> if(acc._2 >= count._2) acc else count)
      .sortBy(_._1.hashCode()).map(w=>(w._1.toString,w._2._1.toString))

  }

  def apply(fileName:String): RDD[(String,String)] =  {
    operate(fileName)
  }
}
