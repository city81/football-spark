name := "football-spark"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.1.0",
    "org.scalatest" % "scalatest_2.11" % "2.2.1",
    "org.slf4j" % "slf4j-api" % "1.7.5"
  )
}