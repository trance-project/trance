import sbt.Keys._
import play.sbt.PlaySettings

val reactiveMongoVer = "1.0.0-play26"
val swaggerVer = "1.7.1" // https://github.com/swagger-api/swagger-play

lazy val root = (project in file("."))
  .enablePlugins(PlayService, PlayLayoutPlugin, Common)
  .settings(
    name := "trance-play-framework",
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      guice,
      "org.reactivemongo"       %% "play2-reactivemongo"      % reactiveMongoVer,
      "io.swagger"              %% "swagger-play2"            % swaggerVer,
      "org.webjars"             %  "swagger-ui"               % "3.22.2",
      "org.joda"                % "joda-convert"              % "2.2.1",
      "net.logstash.logback"    % "logstash-logback-encoder"  % "6.2",
      "io.lemonlabs"            %% "scala-uri"                % "1.5.1",
      "net.codingwell"          %% "scala-guice"              % "4.2.6",
      "org.scalatestplus.play"  %% "scalatestplus-play"       % "5.0.0" % Test
    ),
    scalacOptions ++= Seq(
      "-feature",
      "-deprecation",
      "-Xfatal-warnings"
    )
  )

lazy val gatlingVersion = "3.3.1"
lazy val gatling = (project in file("gatling"))
  .enablePlugins(GatlingPlugin)
  .settings(
    scalaVersion := "2.12.13",
    libraryDependencies ++= Seq(
      "io.gatling.highcharts" % "gatling-charts-highcharts" % gatlingVersion % Test,
      "io.gatling" % "gatling-test-framework" % gatlingVersion % Test
    )
  )

// Reactive Scala Driver for MongoDb --> http://reactivemongo.org/releases/0.1x/documentation/tutorial/play.html
libraryDependencies ++= Seq(
  "org.reactivemongo" %% "play2-reactivemongo" % "0.20.13-play27"
)






