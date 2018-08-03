name := "spark-problem"

version := "1.0"

scalaVersion := "2.11.11"

// The necessary dependencies can be added here
libraryDependencies ++= {
  val sparkVersion = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.scalatest" %% "scalatest" % "3.0.4" % "test"
  )
}