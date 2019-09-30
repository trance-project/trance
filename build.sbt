name := "shredder"

version := "0.1"

scalaVersion := "2.12.7"
val sparkVersion = "2.4.2"

unmanagedBase := baseDirectory.value / "libs"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.4"
//libraryDependencies += "com.github.samtools" % "htsjdk" % "2.9.1"
