package main.scala.com.sundogsoftware.spark.codealongs

import org.apache.log4j._
import org.apache.spark._

object TotalAmountSpentRajul {

  def parseLines(line: String): (Int, Float) = {
    val lines = line.split(",")
    val custId = lines(0).toInt
    val amount = lines(2).toFloat
    (custId, amount)
  }

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalAmountSpentRajul")

    val lines = sc.textFile("data/customer-orders.csv")

    val parsedLines = lines.map(parseLines)

    val filteredLines = parsedLines.reduceByKey( (x,y) => x+y )

    val result = filteredLines.sortByKey().collect()
    result.foreach(println)
  }
}
