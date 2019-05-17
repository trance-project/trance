name := "shredder"

version := "0.1"

scalaVersion := "2.12.7"

val sparkVersion = "2.4.0"

//assemblyMergeStrategy in assembly := {
// case PathList("META-INF", xs @ _*) => MergeStrategy.discard
// case x => MergeStrategy.first
//}

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

//libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

//libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"

//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

//libraryDependencies ++= Seq("org.glassfish.hk2" % "hk2-utils" % "2.2.0-b27",
//  "org.glassfish.hk2" % "hk2-locator" % "2.2.0-b27",
//  "javax.validation" % "validation-api" % "1.1.0.Final")
