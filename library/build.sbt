name := "library"

version := "0.1"

scalaVersion := "2.12.8"
val sparkVersion = "3.3.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % sparkVersion
libraryDependencies += "framework" % "framework_2.12" % "0.1"