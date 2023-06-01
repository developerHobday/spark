package com.hobday

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.types._

object HousingML {
  case class RegressionSchema(
    No: Integer,
    TransactionDate: Double,
    HouseAge: Double,
    DistanceToMRT: Double,
    NumberConvenienceStores: Integer,
    Latitude: Double,
    Longitude: Double,
    PriceOfUnitArea: Double
  )

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("HousingML")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header", "true"
      .option("inferSchema", "true"))
      .csv("data/regression.csv")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("HouseAge", "DistanceToMRT", "NumberConvenienceStores")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
        .select("label","features")
     
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)
    
    val tree = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")
    
    val model = tree.fit(trainingDF)
    val fullPredictions = model.transform(testDF).cache()
    val predictionAndLabel = fullPredictions.select("prediction", "label").collect()
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    spark.stop()
  }
}