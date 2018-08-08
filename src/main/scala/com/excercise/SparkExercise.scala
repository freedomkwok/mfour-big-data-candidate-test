package com.excercise

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.collection.mutable.ListBuffer

object SparkExercise extends Serializable {
  def main(arg: Array[String]) {

    var log = Logger.getLogger(getClass.getName)

    def parseHouseRecord (line:String)= {
      val fields = line.split(",")
      val street = fields(0)
      val sale_date = fields(8)
      ((street, sale_date), 1) //reduce count
    }

    def parseCrimeRecord (line:String):((String, String, String), Int) = {
      val fields = line.split(",")
      val cdatetime = fields(0)
      val address = fields(1)
      val ucr_ncic_code = fields(6)
      ((address, cdatetime, ucr_ncic_code), 1)
    }

    def makeCrimeVert(address :String): Option[(String)] = {
          return Some(address)
    }

    def makeEdges(houseRow :(String, String)): List[Edge[String]] = {
      var edges = new ListBuffer[Edge[String]]()
      val address = houseRow._1.toString
      edges += Edge(address, 0, 0)
    }

    try {
      val sc = new SparkContext("local[*]", "Exercise1")
      val linesOfCrimes = sc.textFile("./src/main/resources/exercise1/SacramentocrimeJanuary2006.csv")
      val crimeRecords = linesOfCrimes.map(parseCrimeRecord).reduceByKey(_ + _)
      val totalCrime = crimeRecords.count
      val verts = crimeRecords.flatMap(x => makeCrimeVert(x._1.toString()))


      log.info(totalCrime)

      val linesOfHouse = sc.textFile("./src/main/resources/exercise1/Sacramentorealestatetransactions.csv")
      val houseRecords = linesOfHouse.map(parseHouseRecord).reduceByKey(_ + _)
      val totalHouses = houseRecords.count

      val default = "Nobody"

      log.info(totalHouses)
    }
    catch {
      case ex: Exception =>
        log.error("General error", ex)
        throw ex
    }
  }

}