name := "sprkloader"

version := "0.1"

scalaVersion := "2.12.7"
val sparkVersion = "2.4.2"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "com.github.samtools" % "htsjdk" % "2.9.1"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.2_0.12.0" % Test
