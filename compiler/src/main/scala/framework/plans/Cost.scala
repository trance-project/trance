package framework.plans

import scala.sys.process._
import scala.language.postfixOps
import framework.generator.spark._
import java.io._
import java.util.UUID.randomUUID

case class Statistics(sizeInBytes:String, rowCount:String, hints:String)

object Cost {

  val StatsRegex = "Statistics\\((.*),(.*),(.*)\\)".r
  

  def readStats(s: String): Option[Statistics] = s match {
    case StatsRegex(sb, rc, h) => Some(Statistics(sb, rc, h))
    case _ => None
  }

  def readOutput(s: String): Option[Statistics] =    
    StatsRegex.findFirstIn(s) match {
      case Some(str) => readStats(str)
      case _ => None
    }

  def generateSpark(plans: List[CNamed]): Unit = {
    val generator = new SparkDatasetGenerator(false, false, evalFinal=false)
    val anfBase = new BaseOperatorANF{}
    var gcode = ""
    // todo could add map to avoid duplicate calls
    var inc = 0
    for (p <- plans){
      val anfer = new Finalizer(anfBase)
      val anfed = anfBase.anf(anfer.finalize(p).asInstanceOf[anfBase.Rep])
      gcode += s"""
        | def g$inc {
        |   ${generator.generate(anfed)}
        |   println("${p.name}",${p.name}.queryExecution.optimizedPlan.stats)
        | }
        | g$inc
        """
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
    val fconts = writeApplication("GenerateCosts", data, ghead, gcode, genc)
    printer.println(fconts)
    printer.close
  }

  // todo reassociate these costs to the ce and se's 
  def getCost(plans: List[CE]): Unit = {
    val cnames = plans.flatMap{
      case ce => 
        val ceName = s"""Cost${randomUUID().toString().replace("-", "")}"""
        val namedSes = ce.ses.map(s => CNamed(s"""Cost${randomUUID().toString().replace("-", "")}""", s.subplan))
        CNamed(ceName, ce.cover) +: namedSes
    }
    generateSpark(cnames) 
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
    val costs = "sh compile.sh".!!
    println(costs)
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
      |import sparkutils._
      |import sparkutils.loader._
      |import sparkutils.skew.SkewDataset._
      |$header
      |object $appname {
      | def main(args: Array[String]){
      |   val conf = new SparkConf()
      |     .setAppName("${appname}Cost")
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $encoders
      |   import spark.implicits._
      |   $data
      |   $gcode
      | }
      |}""".stripMargin
  }



}