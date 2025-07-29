package com.spark.tutorials

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, lit}

class Analysis extends BaseSparkSession {

  case class InputDataSet(df: DataFrame) extends DataframeSet
  case class OutputDataSet(df: DataFrame) extends DataframeSet
  var result:DataFrame = null
  override protected def loadData(inputPath: String): DataframeSet = {

    InputDataSet(spark.read.parquet(s"$inputPath/*.parquet"))

  }

  override protected def process[T <: DataframeSet](dfSet: T) = {
    dfSet match {
      case dfs: InputDataSet => {
        result = dfs.df.groupBy("PULocationID")
          .agg(
            avg("tip_amount"),
            count(lit(1)).as("num_rows")
          )
        result.sort(col("num_rows").desc).show(truncate = false, numRows = 10)
      }
    }

  }

  override def save(outputPath: String): Unit = {
    result.write.option("header", "true").csv(outputPath)
  }


}