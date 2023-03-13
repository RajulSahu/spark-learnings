package main.scala.com.sundogsoftware.spark.codealongs

import org.apache.spark._
import org.apache.log4j._
import scala.math.max

object NewMaxTemperature {

  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat
    (stationId, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "NewMaxTemperature")

    val lines = sc.textFile("data/1800.csv")

    val parsedLines = lines.map(parseLine)

    val filteredLines = parsedLines.filter(x => x._2 == "TMAX")

    val stationTemps = filteredLines.map(x => (x._1, x._3))

    val maxTempByStations = stationTemps.reduceByKey((x,y) => max(x,y))

    val results = maxTempByStations.collect()

    for(result <- results.sorted) {
      println(s"The station code is: ${result._1} and Max Temperature is: ${result._2}")
    }
  }
}
