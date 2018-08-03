package org.spark.app.spark

import org.apache.spark.sql.SparkSession

//provide spark session for application
object SparkCommons {

  lazy val spark = SparkSession.builder
                  .master("local[*]")
                  .appName("spark-problem")
                  .config("spark.logConf", "true")
                  .getOrCreate()
}
