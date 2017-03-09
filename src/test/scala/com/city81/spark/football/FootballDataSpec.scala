package com.city81.spark.football

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSpec, GivenWhenThen}

class FootballDataSpec extends FunSpec with GivenWhenThen {

  implicit val conf = new SparkContext(new SparkConf().setAppName("Football Data").setMaster("local"))

  var footballData: FootballData = new FootballData

  describe("A Football Data processor") {

    it("should find the most common occurrence") {

      Given("a football data file")
      val file = "./src/test/resources/1516E0.csv"

      When("a referee filter is applied")
      val filterColumn = "Referee"

      Then("the most occurring referee is found")
      assertResult("M Atkinson") {
        footballData.filterAndSort(file, filterColumn).first()._1
      }

    }

    it("should find the number of fixtures") {

      Given("a football data file")
      val file = "./src/test/resources/1516E0.csv"

      When("a home odds filter is applied and results are grouped")
      val filterColumn = "BbAvH"
      val filterCriteria = (2.0, 2.2)
      val groupColumn = "FTR"

      Then("the most occurring result is found")
      assertResult(Array(("H", 12), ("D", 9), ("A", 5))) {
        footballData.filterAndGroup(List(file), filterColumn, groupColumn, filterCriteria).collect()
      }

    }

    it("should find the number of fixtures over several seasons") {

      Given("a football data file")
      val dir = "./src/test/resources/"
      val files = List(dir + "1516E0.csv", dir + "1415E0.csv", dir + "1314E0.csv")

      When("a home odds filter is applied and results are grouped")
      val filterColumn = "BbAvH"
      val filterCriteria = (1.6, 1.8)
      val groupColumn = "FTR"

      Then("the most occurring result is found")
      assertResult(Array(("H", 40), ("D", 22), ("A", 16))) {
        footballData.filterAndGroup(files, filterColumn, groupColumn, filterCriteria).collect()
      }

    }

    it("should find all the home team occurrences") {

      Given("a football data file")
      val file = "./src/test/resources/1516E0.csv"

      When("a home team filter is applied")
      val team = "Arsenal"

      And("a probability file is supplied")
      val probFile = "./src/test/resources/1516E0_probability.csv"

      val result = footballData.combineFiles(file, team, probFile)

      Then("the first line will be Arsenal v West Ham")
      assertResult("E0,09/08/15,Arsenal,West Ham,1.3,3.7,9.5") {
        result.first()._1
      }
      assertResult("E0,09/08/15,Arsenal,West Ham,0,2,A,0,1,A,M Atkinson,22,8,6,4,12,9,5,4,1,3,0,0,1.29,6,12,1.28,5.75,10.5,1.33,4.8,8.3,1.29,5.5,12,1.31,5.75,12,1.3,5,11,1.3,5.75,12,45,1.33,1.29,6.16,5.53,12,10.75,39,1.71,1.64,2.3,2.22,27,-1.5,1.96,1.89,2.06,1.96") {
        result.first()._2
      }
    }

  }

}