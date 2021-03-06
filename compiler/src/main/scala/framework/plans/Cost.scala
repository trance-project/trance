package framework.plans

import scala.sys.process._
import scala.language.postfixOps
import framework.generator.spark._
import java.io._
import java.util.UUID.randomUUID
import scala.collection.mutable.Map
import scala.io.Source

case class Statistics(sizeInBytes:String, rowCount:String, hints:String)

object Cost {

  val nameMap = Map.empty[String, String]
  val nameMapRev = Map.empty[String, String]
  val codeMap = Map.empty[String, String]
  val statsMap = Map.empty[String, Statistics]

  val StatsRegex = "Stat\\((.*),(.*),(.*),(.*)\\)".r

  def readStats(s: String): (String, Statistics) = s match {
    case StatsRegex(n, sb, rc, h) => (n, Statistics(sb, rc, h))
    case _ => ("null", Statistics("null", "null", "null"))
  }

  def readOutput(s: String): (String, Statistics) =    
    StatsRegex.findFirstIn(s) match {
      case Some(str) => readStats(str)
      case _ => ("null", Statistics("null", "null", "null"))
    }

  def generateSpark(plans: List[CNamed]): Unit = {
    val generator = new SparkDatasetGenerator(false, false, evalFinal=false)
    // var gcode = ""
    // todo could add map to avoid duplicate calls
    var inc = 0
    for (p <- plans){
      val anfBase = new BaseOperatorANF{}
      val anfer = new Finalizer(anfBase)
      val anfed = anfBase.anf(anfer.finalize(p).asInstanceOf[anfBase.Rep])
      val gcode = s"""
        | /** ${Printer.quote(p)} **/
        | def g$inc {
        |   ${generator.generate(anfed)}
        |   val stat = ${p.name}.queryExecution.optimizedPlan.stats
        |   println(genStat("${p.name}", stat))
        | }
        | g$inc
        """
      codeMap += (p.name -> gcode)
      inc += 1
    }
    val ghead = generator.generateHeader()
    val genc = generator.generateEncoders()
    val fname = "../executor/spark/src/main/scala/sparkutils/generated/GenerateCosts.scala"
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val data = s"""
      |   val copynumber = spark.table("copynumber")
      |   val occurrences = spark.table("occurrences")
      |   val samples = spark.table("samples")
      """
    val fconts = writeApplication("GenerateCosts", data, ghead, codeMap.map(_._2).mkString("\n"), genc)
    printer.println(fconts)
    printer.close
  }

  def getUUID: String = randomUUID().toString().replace("-", "")

  def updateNameMap(key: String, value: String): String = {
    if (nameMap contains key) nameMap(key)
    else {
      nameMap += (key -> value)
      nameMapRev += (value -> key)
      value
    }
  }

  // todo reassociate these costs to the ce and se's 
  def getCost(plans: List[CE]): Unit = {
    val cnames = plans.flatMap{
      case ce => 
        val ceName = updateNameMap(ce.cover.vstr, s"Cost${getUUID}")
        val namedSes = ce.ses.map(s => {
            val seName = updateNameMap(s.subplan.vstr, s"Cost${getUUID}")
            CNamed(seName, s.subplan)
          })
        CNamed(ceName, ce.cover) +: namedSes
    }
    runCost(cnames) 
  }

  // uses spark-shell, which is super slow
  // def getStats(plan: CNamed): Option[String] = {
  //   generateSpark(plan)
  //   val str = "spark-shell < test.scala" .!!
  //   StatsRegex.findFirstIn(str)
  // }

  // this is a slightly faster solution
  def runCost(plans: List[CNamed]): Unit = {
    generateSpark(plans)
    "sh compile.sh".!!
    val costs = "cat out".!!
    for (line <- Source.fromFile("out").getLines){
      val (name, stat) = readStats(line)
      statsMap += (nameMapRev(name) -> stat)
    }
    println(statsMap)
  }

    /** Writes a generated application for a query using Spark Datasets **/
  def writeApplication(appname: String, data: String, header: String, gcode: String, encoders: String): String  = {
    s"""
      |package sparkutils.generated
      |/** Generated **/
      |import org.apache.spark.SparkConf
      |import org.apache.spark.sql.SparkSession
      |import org.apache.spark.sql._
      |import org.apache.spark.sql.functions._
      |import org.apache.spark.sql.catalyst.plans.logical._
      |import sparkutils._
      |import sparkutils.loader._
      |import sparkutils.skew.SkewDataset._
      |$header
      |case class Stat(name: String, sizeInBytes:String, rowCount:String, hints:String)
      |object $appname {
      | def main(args: Array[String]){
      |   val conf = new SparkConf()
      |     .setAppName("${appname}Cost")
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $encoders
      |   import spark.implicits._
      |   def genStat(n: String, s: Statistics): Stat = s.rowCount match {
      |     case Some(rc) => Stat(n, s.sizeInBytes.toString, rc.toString, s.hints.toString)
      |     case _ => Stat(n, s.sizeInBytes.toString, "-1", s.hints.toString)
      |   }
      |   $data
      |   $gcode
      | }
      |}""".stripMargin
  }



}