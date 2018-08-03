package org.spark.app.scala

import org.apache.spark.rdd.RDD
import org.spark.app.scala.operations._

import scala.util.Try
/*
 Strategy pattern based class
 containing all the strategies of operation
 */
object Solutions {

  object Type extends Enumeration {
    type Type = Value
    val first = "1"
    val second = "2"
    val third = "3"
  }

}

case class SolutionStrategy(choice:String, file:String)

/*
object having defined method for each strategy
based on selection from command line
 */
object Strategies {
  type Strategy[T, O] = T => O

  def ReduceByKeyStrategy[T](): Strategy[T, Try[RDD[(String,String)]]] =
    (t: T) => {
      val result = t.asInstanceOf[SolutionStrategy]
      Try(ReduceByKeyOp(result.file))
    }

  def TopByKeyStrategy[T](): Strategy[T, Try[RDD[(String,String)]]] =
    (t: T) => {
      val result = t.asInstanceOf[SolutionStrategy]
      Try(TopByKeyOp(result.file))
    }

  def FoldByKeyStrategy[T](): Strategy[T, Try[RDD[(String,String)]]] =
    (t: T) => {
      val result = t.asInstanceOf[SolutionStrategy]
      Try(FoldByKeyOp(result.file))
    }

  def selectStrategy[T](choice:String):Strategy[T, Try[RDD[(String,String)]]] =
    choice match {
      case Solutions.Type.first=> {
        ReduceByKeyStrategy()
      }
      case Solutions.Type.second=> {
        TopByKeyStrategy()
      }
      case Solutions.Type.third=> {
        FoldByKeyStrategy()
      }
      case _ => {
        sys.exit(1)
      }
    }

}
