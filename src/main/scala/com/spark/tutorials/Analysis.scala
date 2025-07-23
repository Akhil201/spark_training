package com.spark.tutorials

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, lit}

class Analysis extends BaseSparkSession {

  override protected def loadData(inputPath: String): DataFrame = {
    spark.read.parquet(s"$inputPath/*.parquet")
  }

  override def process(df: DataFrame): DataFrame = {
    val result = df
      .groupBy("PULocationID")
      .agg(
        avg("tip_amount"),
        count(lit(1)).as("num_rows")
      )
    result.sort(col("num_rows").desc).show(truncate = false, numRows = 10)
    result
  }
}