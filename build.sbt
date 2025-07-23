name := "spark_training"

scalaVersion := "2.13.16"

name := "spark_training"
organization := "com.spark"
version := "1.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.0"
val sparkVersion = "3.2.3"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)