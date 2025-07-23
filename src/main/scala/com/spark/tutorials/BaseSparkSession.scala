package com.spark.tutorials

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseSparkSession {

  protected val spark: SparkSession = createSparkSession("Spark test", isLocal = true)

  // Hook methods to be overridden
  protected def loadData(inputPath: String): DataFrame

  protected def process(df: DataFrame): DataFrame

  protected def save(df: DataFrame, outputPath: String): Unit = {
    df.write.option("header", "true").csv(outputPath)
  }

  final def run(inputPath: String, outputPath: String): Unit = {
    val df = loadData(inputPath)
    val result = process(df)
    save(result, outputPath)
    spark.stop()
  }


  private def createSparkSession(appName: String, isLocal: Boolean): SparkSession = {
    if (isLocal) {
      SparkSession
        .builder()
        .config("spark.sql.caseSensitive", value = true)
        .config("spark.sql.session.timeZone", value = "UTC")
        .config("spark.driver.memory", value = "8G")
        .appName(appName)
        .master("local[*]")
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .config("spark.sql.caseSensitive", value = true)
        .config("spark.sql.session.timeZone", value = "UTC")
        .appName(appName)
        .getOrCreate()
    }
  }
}
