name := "library"

version := "0.1"

scalaVersion := "2.12.8"
val sparkVersion = "3.3.2"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.3"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.2"
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % sparkVersion
libraryDependencies += "framework" % "framework_2.12" % "0.1"
//libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.3.2_1.4.2" % "test"

testOptions += Tests.Argument("-oF")