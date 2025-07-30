package com.spark.tutorials

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, avg, col, concat_ws, count, explode, lit, rand}

class Analysis extends BaseSparkSession {

  case class InputDataSet(df: DataFrame, locationDF: DataFrame) extends DataframeSet
  case class OutputDataSet(df: DataFrame) extends DataframeSet
  var result:DataFrame = null
  override protected def loadData(inputPath: String): DataframeSet = {

    InputDataSet(spark.read.parquet(s"$inputPath/*.parquet"), spark.read.option("header", "false")
      .csv(s"""$inputPath/tripdatasmall.csv"""))

  }

  override protected def process[T <: DataframeSet](dfSet: T) = {
    dfSet match {
      case dfs: InputDataSet => {
        val parquetDF = dfs.df
        val locationDF = dfs.locationDF
        parquetDF.printSchema()
        parquetDF.createOrReplaceTempView("parquetDF")
        locationDF.createOrReplaceTempView("locationDF")
        val locationDFSch = locationDF.toDF("VendorID", "LOCATION")

        spark.sql("select count(1) from parquetDF").show()
        spark.sql("select VendorID, count(1) from parquetDF group by VendorID").show()
        spark.sql("select count(1) from locationDF").show()

        // here (rand() * saltRange) will generate numbers between 0 to 9
        val saltRange = 10
        val parquetDFWithSalt = parquetDF
          .withColumn("salt", (rand() * saltRange).cast("int"))
          .withColumn("salted_key", concat_ws("_", col("VendorID"), col("salt")))
        parquetDFWithSalt.select("salted_key").distinct().show()


        // Create salt array
        val saltArray = (0 until saltRange).toArray
        val explodedSmallDF = locationDFSch
          .withColumn("salt_array", array(saltArray.map(lit):_*))
          .withColumn("salt", explode(col("salt_array")))
          .withColumn("salted_key", concat_ws("_", col("VendorID"), col("salt")))
          .drop("salt_array")

        parquetDFWithSalt.join(explodedSmallDF, Seq("salted_key"),"inner")
          .groupBy(explodedSmallDF("salted_key")).agg(count("*")).show()


//        result = dfs.df.groupBy("PULocationID")
//          .agg(
//            avg("tip_amount"),
//            count(lit(1)).as("num_rows")
//          )
//        result.sort(col("num_rows").desc).show(truncate = false, numRows = 10)
      }
    }

  }

  override def save(outputPath: String): Unit = {
//    result.write.option("header", "true").csv(outputPath)
  }


}