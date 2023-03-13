package main.scala.com.sundogsoftware.spark.codealongs


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsByAge {

  case class FakeFriends(id:Int, name:String, age:Int, friends:Int)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("friendsByAge")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val friends = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/fakefriends.csv")
      .as[FakeFriends]

    val friendsAgeAndNumberOfFriendsSelected = friends.select(friends("age"), friends("friends"))


    friendsAgeAndNumberOfFriendsSelected.groupBy("age").avg("friends").show()

    friendsAgeAndNumberOfFriendsSelected
      .groupBy("age")
      .agg(round(avg("friends"), 2))
      .sort("age")
      .show()




  }

}
