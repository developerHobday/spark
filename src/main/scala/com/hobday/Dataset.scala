package com.hobday

import org.apache.spark.sql._
import org.apache.log4j._
    
object Dataset {  
  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]) {    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/friends.csv")
      .as[Person]

    ds.printSchema()
    ds.select("name").show()
    ds.filter(people("age") < 21).show()
    ds.groupBy("age").count().show()
    ds.select(people("name"), people("age") + 10).show()

    val friendsByAge = ds.select("age", "friends")
    friendsByAge.groupBy("age").agg(
      round(avg("friends"), 2).alias("friends_avg")
    ).sort("age").show()

    
    spark.stop()
  }
}