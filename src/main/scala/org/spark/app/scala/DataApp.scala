package org.spark.app.scala

import org.spark.app.scala.util.FileUtil

import scala.util.{Failure, Success}

/** object having main method for application
  * provide command line interface
  * accepts 2 command line arguments args[0], args[1]
  * args[0]: input file to be processed
  * args[1]: output location with or without file name
  */
object DataApp extends App {


  if(args.length < 2) {
    println(
      """File locations parameter is missing
        |shall have 2 parameter in below order
        |<File.scala> <1st parameter: input file> <2nd parameter: output location>
        |2nd paramater: default filename is 'result', if filename is not provided
      """.stripMargin)
  }
  else {
    val fileName= args(0).trim
    val outputLoc = args(1).trim
    println(
      """Enter choice:
        |1. ReduceByKeyOp
        |2. TopByKeyOp
        |3. FoldByKeyOp
        |press any other no to exit
      """.stripMargin)
    val choice= scala.io.StdIn.readLine().trim
    val method= SolutionStrategy(choice, fileName)
    val process= Strategies.selectStrategy[SolutionStrategy](choice)
    process(method) match {
      case Success(rdd)=> FileUtil.writeToCsvFile(rdd,outputLoc)
      case Failure(ex)=> println("ERROR occurred: "+ex.getMessage)
    }

  }


}
