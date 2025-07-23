package com.spark.tutorials

object SparkMainRunner extends App {


  val (className, inputPath, outputPath) = parseArgs(args = args)
  private val clazz = Class.forName(className)
  private val obj = clazz.getDeclaredConstructor().newInstance()
  private val mainMethod = clazz.getMethod("run", classOf[String], classOf[String])
  // Call static main method with remaining args
  mainMethod.invoke(obj, inputPath, outputPath);

  private def parseArgs(args: Array[String]): (String, String, String) = {
    val className = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    println(s"className: ${className}")
    println(s"input path: ${inputPath}")
    println(s"output path: ${outputPath}")
    (className, inputPath, outputPath)
  }


}
