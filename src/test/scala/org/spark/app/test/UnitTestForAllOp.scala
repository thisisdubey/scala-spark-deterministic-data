import org.scalatest.FunSuite
import org.spark.app.scala.operations.{FoldByKeyOp, ReduceByKeyOp, TopByKeyOp}

// unit test cases covering basic functionality for each operation
class UnitTestForAllOp extends FunSuite {
  private val sparkSession = org.spark.app.spark.SparkCommons.spark
  private val inputFile="src/test/resources/input_data.csv"
  private val outputFile="src/test/resources/output_data.csv"

  private val expectedRDD = sparkSession.read.format("com.databricks.spark.csv").option("header", "false")
                              .load(outputFile).rdd.map(x=>(x(0).toString, x(1).toString))

  test("test ReduceByKeyOp"){
    val resultRDD = ReduceByKeyOp(inputFile)
    assert(expectedRDD.collect() === resultRDD.collect())
    resultRDD.unpersist(true)
  }

  test("test TopByKeyOp"){
    val resultRDD = TopByKeyOp(inputFile)
    assert(expectedRDD.collect() === resultRDD.collect())
    resultRDD.unpersist(true)
  }

  test("test FoldByKeyOp"){
    val resultRDD = FoldByKeyOp(inputFile)
    assert(expectedRDD.collect() === resultRDD.collect())
    resultRDD.unpersist(true)
  }

}
