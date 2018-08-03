package org.spark.app.scala.operations

import org.spark.app.spark.SparkCommons.spark
import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
import org.apache.spark.rdd.RDD

/**
  * object which operates on input filename
  * using reduce by key to get the total count against each key
  * then top by key (from spark mLib) to pick the top 1 score for each key
  * and reduce by key to pick the largest value against key
  * in case of same (color, count)- pick the first one encountered
  */
object TopByKeyOp {

  def operate(fileName:String): RDD[(String,String)] = {
    val input= spark.read.format("com.databricks.spark.csv").option("header", "false").load(fileName)

    input.rdd.cache().map(w=>(w,1)).reduceByKey(_+_).topByKey(1).map(w=>(w._1(0),(w._1(1),w._2.head)))
      .reduceByKey((x,y)=>if(x._2>=y._2) x else y)
      .sortBy(_._1.hashCode()).map(w=>(w._1.toString,w._2._1.toString))

  }

  def apply(fileName:String): RDD[(String,String)] =  {
    operate(fileName)
  }
}
