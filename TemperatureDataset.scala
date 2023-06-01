package com.hobday

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TemperatureDataset {
  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("Temperature")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)
    import spark.implicits._
    val ds = spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[Temperature]
    
    val maxTemps = ds.filter($"measure_type" === "TMAX")
    val stationTemps = maxTemps.select("stationID", "temperature")
    val maxTempsByStation = stationTemps.groupBy("stationID").max("temperature").alias("maxTemp")
    val maxTempsByStationF = maxTempsByStation
      .withColumn("temperature", round(
        $"maxTemp" * 0.1f * (9.0f / 5.0f) + 32.0f, 2
      )).select("stationID", "temperature")
      .sort("temperature")

    val results = maxTempsByStationF.collect()    
    for (result <- results) {
       val station = result(0)
       val temp = result(1).asInstanceOf[Float]
       val formattedTemp = f"$temp%.2f F"
       println(s"$station max temperature: $formattedTemp")
    }
  }
}