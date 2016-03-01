name := "football-spark"

version := "1.0"

scalaVersion := "2.11.7"

// additional libraries
libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.5"
  )
}