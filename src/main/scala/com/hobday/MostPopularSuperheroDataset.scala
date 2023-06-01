package com.hobday

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SuperheroDataset {
  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)
 
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MostPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]
    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostPopular = connections
      .sort($"connections".desc)
      .head(10)
    val minConnectionCount = connections.agg(min("connections")).first().getLong(0)
    val minConnections = connections.filter( $"connections" === minConnectionCount
      ).join(names, usingColumn = "id")

    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    println("Most popular")
    mostPopular.join( names, usingColumn = "id"
      ).select("name"
      ).show()

    println("Least popular")
    minConnections.join( names, usingColumn = "id"
      ).select("name"
      ).show()
    
  }
}
