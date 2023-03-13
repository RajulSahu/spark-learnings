package main.scala.com.sundogsoftware.spark.codealongs

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataFrameDemo {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    var people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]


    println("Here is the inferred schema")
    people.printSchema()

    people.select("name").show()

    people.select(people("name"), people("age")).show()
    //or
    people.select("name", "age").show()

    people.groupBy("age").count().show()

    people.select(people("name"), people("age") + 10).show()


    people.select(people("name"), people("age")).where(people("age") < 20).show()
    // or
    people.select(people("name"), people("age")).where("age < 20").show()

    spark.stop()
  }

}
