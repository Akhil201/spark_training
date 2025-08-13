///*
//package com.spark.tutorials
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import java.util.Arrays
//object SparkTest extends App {
//
//  val spark = SparkSession
//    .builder()
//    .config("spark.sql.caseSensitive", value = true)
//    .config("spark.sql.session.timeZone", value = "UTC")
//    .config("spark.driver.memory", value = "8G")
//    .appName("test")
//    .getOrCreate()
//  val customersDataFrame = spark.read
//    .option("header", "true").option("delimiter", "~")
//    .csv(s"""transactions.csv""")
//
//  customersDataFrame.withColumn("detailedTransactions", explode(split(col("Transaction"), ",")))
//    .withColumn("detailedAMount", explode(split(col("Amount"), ",")))
//
//  val lst = List(1,3,6,2,7)
//  lst.sorted
//
//}
//*/
