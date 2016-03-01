package com.city81.spark.football

import org.apache.spark._
import org.apache.spark.rdd.RDD


class FootballData {

  def sort(file: String, filterColumn: String)(implicit sparkContext: SparkContext): RDD[(String, Int)] = {

    val csvFile = sparkContext.textFile(file)
    val data = csvFile.map(line => line.split(",").map(elem => elem.trim))
    val header = new FootballCSVHeader(data.take(1)(0))
    data.filter(line => header(line, filterColumn) != filterColumn)
      .map(row => header(row, filterColumn))
      .map(word => (word, 1)).reduceByKey(_ + _)
      .sortBy(_._2, false)
  }

}