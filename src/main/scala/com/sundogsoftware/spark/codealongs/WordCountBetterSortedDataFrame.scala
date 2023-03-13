package main.scala.com.sundogsoftware.spark.codealongs

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WordCountBetterSortedDataFrame {

  case class Book(value:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("WordCountBetter")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    val bookData = spark.read.text("data/book.txt").as[Book]

    val words = bookData
      .select(explode(split($"value", "\\W+")).alias("words"))
      .filter("words != ''")

    val lowerCaseWords = words.select(lower($"words").alias("words"))

    val wordCount = lowerCaseWords.groupBy("words").count()

    val wordCountsSorted = wordCount.sort("count")

    wordCountsSorted.show(wordCountsSorted.count().toInt)

    spark.stop()

  }

}
