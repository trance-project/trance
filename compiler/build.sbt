name := "framework"

version := "0.1"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.2"

libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.13"


libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0-M3"
