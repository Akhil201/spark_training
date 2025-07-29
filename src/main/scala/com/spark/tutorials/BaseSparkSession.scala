package com.spark.tutorials

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataframeSet

abstract class BaseSparkSession {

  protected val spark: SparkSession = createSparkSession("Spark test", isLocal = true)

  // Hook methods to be overridden
  protected def loadData(inputPath: String): DataframeSet

  protected def process[T <: DataframeSet](dfSet: T): Unit

  protected def save(outputPath: String): Unit

  final def run(inputPath: String, outputPath: String): Unit = {
    val dfSet = loadData(inputPath)
    process(dfSet)
    save(outputPath)
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
