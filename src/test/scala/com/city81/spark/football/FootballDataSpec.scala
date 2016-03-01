package com.city81.spark.football

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{FunSpec, GivenWhenThen}

class FootballDataSpec extends FunSpec with GivenWhenThen {

  implicit val conf = new SparkContext(new SparkConf().setAppName("Football Data").setMaster("local"))

  var footballData: FootballData = new FootballData

  describe("A Football Data filter") {

    it("should find the most common occurrence") {

      Given("a football data file")
      val file = "./src/main/resources/1516E0.csv"

      When("a referee filter is applied")
      val filterColumn = "Referee"

      Then("the most occurring referee is found")
      assertResult("M Atkinson") {
        footballData.sort(file, filterColumn).first()._1
      }

    }
  }

}