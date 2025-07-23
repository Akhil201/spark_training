package com.spark.tutorials

import com.spark.tutorials.Analysis._
import sparktutorial._


object SparkMainRunner extends App {
  val spark = createSparkSession("Spark test", isLocal = true)

  val (inputPath, outputPath) = parseArgs(args = args)
  val data = spark.read.parquet(s"${inputPath}/*.parquet")

  val analysisResult = calculateAverageTipByPickupLocation(data = data)

  analysisResult.write.option("header", "true").csv(outputPath)
}
