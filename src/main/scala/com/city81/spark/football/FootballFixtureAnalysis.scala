package com.city81.spark.football

import java.util.Calendar

import org.apache.spark._


/**
  * Process a fixture file and for past games with similar odds, output the frequency of each result
  */
object FootballFixtureAnalysis extends App {

  val conf = new SparkContext(new SparkConf().setAppName("Football Data").setMaster("local"))
  conf.setLogLevel("ERROR")

  val dir = "./src/test/resources/"

  val fixtureFile = dir + "fixtures.csv"

  val dateFormat = new java.text.SimpleDateFormat("dd/MM/yy")

  val fixMap = scala.collection.mutable.Map[String, List[(String, Double, Double)]]()

  val fixtureCsvFile = conf.textFile(fixtureFile).cache()

  val fixtureData = fixtureCsvFile.map(line => line.split(",").map(elem => elem.trim)).collect()

  for (fix <- fixtureData.drop(1)) {

    val league = fix(0)

    val historicalFiles = List(
      dir + "1617" + league + ".csv",
      dir + "1516" + league + ".csv",
      dir + "1415" + league + ".csv",
      dir + "1314" + league + ".csv")

    val homeOdds = fix(33).toDouble
    val drawOdds = fix(35).toDouble
    val awayOdds = fix(37).toDouble

    val lowerHome = lowerBound(homeOdds)
    val higherHome = higherBound(homeOdds)
    val lowerDraw = lowerBound(drawOdds)
    val higherDraw = higherBound(drawOdds)
    val lowerAway = lowerBound(awayOdds)
    val higherAway = higherBound(awayOdds)


    var rdd = conf.emptyRDD[(String, Int)]

    for (file <- historicalFiles) {

      val csvFile = conf.textFile(file).cache()
      val data = csvFile.map(line => line.split(",").map(elem => elem.trim))
      val header = new FootballCSVHeader(data.take(1)(0))

      val homeFilterColPos = header.index("BbAvH")
      val drawFilterColPos = header.index("BbAvD")
      val awayFilterColPos = header.index("BbAvA")

      val fixDate = dateFormat.parse(fix(1))

      val c = Calendar.getInstance()
      c.setTime(fixDate)
      c.add(Calendar.YEAR, -3)
      val rolledFixDate = c.getTime()

      val sortedRDD = data.filter(line => header(line, "BbAvH") != "BbAvH")
        .filter(line => rolledFixDate.before(dateFormat.parse(line(1))))
        .filter(line => (line(homeFilterColPos).toDouble > lowerHome && line(homeFilterColPos).toDouble < higherHome))
        .filter(line => (line(drawFilterColPos).toDouble > lowerDraw && line(drawFilterColPos).toDouble < higherDraw))
        .filter(line => (line(awayFilterColPos).toDouble > lowerAway && line(awayFilterColPos).toDouble < higherAway))
        .map(row => header(row, "FTR"))
        .map(word => (word, 1)).reduceByKey(_ + _).cache()

      rdd = rdd union sortedRDD

    }

    val sortedRDD = rdd.groupBy(_._1).mapValues(_.map(_._2).sum).sortBy(_._1, false)

    println(fix(2) + " v " + fix(3) + " " + sortedRDD.collect().mkString)

  }


  def lowerBound(price: Double): Double = {
    1.0 / ((1.0 / price) + 0.01)
  }

  def higherBound(price: Double): Double = {
    1.0 / ((1.0 / price) - 0.01)
  }

}
