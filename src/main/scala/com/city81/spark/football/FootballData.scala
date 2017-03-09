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

  def filterAndGroup(files: List[String], filterColumn: String, groupColumn: String, filterCriteria: (Double, Double))(implicit sparkContext: SparkContext): RDD[(String, Int)] = {

    val sc = sparkContext
    var rdd = sc.emptyRDD[(String, Int)]

    // files have different formats so can't pass the list into sparkContext.textFile

    for (file <- files) {

      val csvFile = sparkContext.textFile(file).cache()
      val data = csvFile.map(line => line.split(",").map(elem => elem.trim))
      val header = new FootballCSVHeader(data.take(1)(0))
      val filterColumnPos = header.index(filterColumn)

      val sortedRDD = data.filter(line => header(line, filterColumn) != filterColumn)
        .filter(line => (line(filterColumnPos).toDouble > filterCriteria._1 && line(filterColumnPos).toDouble < filterCriteria._2))
        .map(row => header(row, groupColumn))
        .map(word => (word, 1)).reduceByKey(_ + _)

      rdd = rdd union sortedRDD

    }

    // merge tuples together by key ie ("H",20) and ("H",12) becomes ("H",32)
    rdd.groupBy(_._1).mapValues(_.map(_._2).sum).sortBy(_._2, false)

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
    dataRdd.mapPartitions({ iter =>
      val seqRdd1Val = seqRdd1Broadcast.value
      for {
        t <- iter
        m <- seqRdd1Val
        if t.split(",").take(4).sameElements(m.split(",").take(4))
      } yield (m, t)
    }, preservesPartitioning = true)

  }

}