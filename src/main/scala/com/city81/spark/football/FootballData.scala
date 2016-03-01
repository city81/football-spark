package com.city81.spark.football

import org.apache.spark._
import org.apache.spark.rdd.RDD


class FootballData {

  def filterAndSort(file: String, filterColumn: String)(implicit sparkContext: SparkContext): RDD[(String, Int)] = {

    val csvFile = sparkContext.textFile(file).cache()
    val data = csvFile.map(line => line.split(",").map(elem => elem.trim))
    val header = new FootballCSVHeader(data.take(1)(0))
    data.filter(line => header(line, filterColumn) != filterColumn)
      .map(row => header(row, filterColumn))
      .map(word => (word, 1)).reduceByKey(_ + _)
      .sortBy(_._2, false)
  }

  def combineFiles(file: String, team: String, probFile: String)(implicit sparkContext: SparkContext): RDD[(String, String)] = {

    val probCsvFile = sparkContext.textFile(probFile).cache()
    val probData = probCsvFile.map(line => line.split(",").map(elem => elem.trim))
    val probHeader = new FootballCSVHeader(probData.take(1)(0))
    val probDataRdd = probData.filter(line => probHeader(line, "HomeTeam") == team)
      .map(line => line.mkString(","))

    val csvFile = sparkContext.textFile(file).cache()
    val data = csvFile.map(line => line.split(",").map(elem => elem.trim))
    val header = new FootballCSVHeader(data.take(1)(0))
    val dataRdd = data.filter(line => header(line, "HomeTeam") == team)
      .map(line => line.mkString(","))

    val seqRdd1Broadcast = sparkContext.broadcast(probDataRdd.collect())
    val joined = dataRdd.mapPartitions({ iter =>
      val seqRdd1Val = seqRdd1Broadcast.value
      for {
        t <- iter
        m <- seqRdd1Val
        if t.split(",").take(4).sameElements(m.split(",").take(4))
      } yield (m, t)
    }, preservesPartitioning = true)

    joined
  }


}