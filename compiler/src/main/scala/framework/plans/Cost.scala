package framework.plans

import scala.sys.process._
import scala.language.postfixOps
import framework.generator.spark._
import java.io._

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

  def generateSpark(plan: CNamed): Unit = {
    val name = plan.name
    val generator = new SparkDatasetGenerator(false, false, evalFinal=false)
    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val anfed = anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])
    val gplan = generator.generate(anfed)
    val ghead = generator.generateHeader()
    val genc = generator.generateEncoders()
    val fname = "../executor/spark/src/main/scala/sparkutils/generated/GenerateCosts.scala"
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val data = s"""val copynumber = spark.table("copynumber")"""
    val gcode = s"""
      $gplan
      println($name.hashCode()+","+$name.queryExecution.optimizedPlan.stats)
      """
    val fconts = writeApplication("GenerateCosts", data, ghead, gcode, genc)
    printer.println(fconts)
    printer.close
  }

  // def generateCode()

  // uses spark-shell, which is super slow
  def getStats(plan: CNamed): Option[String] = {
    generateSpark(plan)
    val str = "spark-shell < test.scala" .!!
    StatsRegex.findFirstIn(str)
  }

  // this is a slightly faster solution
  def runCost(plan: CNamed): Unit = {
    generateSpark(plan)
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