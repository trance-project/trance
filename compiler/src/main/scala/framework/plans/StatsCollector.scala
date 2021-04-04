package framework.plans

import scala.sys.process._
import scala.language.postfixOps
import framework.generator.spark._
import java.io._
import java.util.UUID.randomUUID
import scala.collection.mutable.Map
import scala.collection.immutable.{Map => IMap}
import scala.io.Source

case class Statistics(sizeInBytes: Long, rowCount: Long) {

  def lessThan(s2: Any): Boolean = s2 match {
    case Statistics(size, rows) => 
      if (rows > -1 && rowCount > -1) sizeInBytes <= size && rowCount <= rows
      else sizeInBytes <= size
    case _ => false
  }

}

class StatsCollector(progs: Vector[(CExpr, Int)]) {

  val nameMap = Map.empty[String, String]
  val nameMapRev = Map.empty[String, String]
  val codeMap = Map.empty[String, String]
  val statsMap = Map.empty[String, Statistics]

  val StatsRegex = "Stat\\((.*),(.*),(.*)\\)".r
  var inc = 0

  def readStats(s: String): (Option[String], Option[Statistics]) = s match {
    case StatsRegex(n, sb, rc) => (Some(n), Some(Statistics(sb.toLong, rc.toLong)))
    case _ => (None, None)
  }

  //TODO
  def readOutput(s: String): (Option[String], Option[Statistics]) =    
    StatsRegex.findFirstIn(s) match {
      case Some(str) => readStats(str) 
      case _ => (None, None)
    }

  def generateSpark(plans: List[CNamed], notebk: Boolean = false): Unit = {
    val generator = new SparkDatasetGenerator(false, false, evalFinal=false)
    // var gcode = ""
    // todo could add map to avoid duplicate calls
    var queries = ""
    for (p <- progs){
      // println(p)
      val name = "Query"+p._2
      val anfBase = new BaseOperatorANF{}
      val anfer = new Finalizer(anfBase)
      val anfed = anfBase.anf(anfer.finalize(p._1).asInstanceOf[anfBase.Rep])
      val gcode = s"""
        | /** ${Printer.quote(p._1)} **/
        | ${generator.generate(anfed)}
        |"""
      queries += gcode
    }

    for (p <- plans){
      // println(p)
      val anfBase = new BaseOperatorANF{}
      val anfer = new Finalizer(anfBase)
      println(Printer.quote(p))
      val anfed = anfBase.anf(anfer.finalize(p).asInstanceOf[anfBase.Rep])
      val gcode = s"""
        | /** ${Printer.quote(p)} **/
        | ${generator.generate(anfed)}
        | val stat$inc = ${p.name}.queryExecution.optimizedPlan.stats
        | println(genStat("${p.name}", stat$inc))
        """
      codeMap += (p.name -> gcode)
      inc += 1
    }
    val ghead = generator.generateHeader()
    val genc = generator.generateEncoders()
    val data = s"""
      |   val copynumber = spark.table("copynumber")
      |   val occurrences = spark.table("occurrences")
      |   val samples = spark.table("samples")
      |   val IBag_copynumber__D = copynumber
      |   val IBag_samples__D = samples
      |   val IBag_occurrences__D = spark.table("odict1")
      |   val IDict_occurrences__D_transcript_consequences = spark.table("odict2")
      |   val IDict_occurrences__D_transcript_consequences_consequence_terms = spark.table("odict3")
      """
    var fname = "../executor/spark/src/main/scala/sparkutils/generated/GenerateCosts"
    val fconts = if (!notebk) {
      fname+=".scala"
      writeApplication("GenerateCosts", data, ghead, codeMap.map(_._2).mkString("\n"), genc)
    }else {
      fname+=".json"
      val pcontents = writeParagraph("GenerateCosts", data, ghead, queries+"\n"+codeMap.map(_._2).mkString("\n"), genc)
      new JsonWriter().buildParagraph("Generated paragraph $qname", pcontents)
    }
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    printer.println(fconts)
    printer.close
  }

  def getUUID: String = randomUUID().toString().replace("-", "")

  def updateNameMap(key: String, value: String): String = {
    if (nameMap contains key) {
      nameMapRev += (value -> key)
      nameMap(key)
    }else {
      nameMap += (key -> value)
      nameMapRev += (value -> key)
      value
    }
  }

  def getCost(plans: List[CE]): Map[String, Statistics] = {
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

  def getCost(subs: Map[Integer, List[SE]], covers: IMap[Integer, CNamed], notebk: Boolean = true): Map[String, Statistics] = {
    val coverList = covers.values.toList
    val plans = getSubs(subs) ++ coverList
    // could handle duplicates better
    coverList.foreach{ ce => updateNameMap(ce.vstr, ce.name) }
    runCost(plans, notebk)
  }

  def getSubs(plans: Map[Integer, List[SE]]): List[CNamed] = {
    plans.values.toList.flatMap{ ses => ses.map{
      case se => 
        val seName = updateNameMap(se.subplan.vstr, s"Cost${getUUID}")
        CNamed(seName, se.subplan)
      }
    }
  }

  def getCoverCost(plans: IMap[Integer, CNamed], notebk: Boolean = true): Map[String, Statistics] = {
    val cnames = plans.values.toList
    cnames.foreach{ ce => updateNameMap(ce.e.vstr, ce.name) }
    runCost(cnames, notebk) 
  }

  // this is a slightly faster solution
  def runCost(plans: List[CNamed], notebk: Boolean = false): Map[String, Statistics] = {
    generateSpark(plans, notebk)
    if (!notebk){
      "sh compile.sh".!!
    }else{
      "sh compile.sh notebk".!!
    }
    // TODO parse better
    for (line <- Source.fromFile("out").getLines){
      readStats(line) match {
        case (Some(name), Some(stat)) => 
          statsMap += (nameMapRev(name) -> stat)
        case _ => 
      }
    }
    statsMap
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
      |import org.apache.spark.sql.types._
      |import org.apache.spark.sql.expressions.scalalang._
      |import sparkutils._
      |import sparkutils.loader._
      |import sparkutils.skew.SkewDataset._
      |$header
      |case class Stat(name: String, sizeInBytes:String, rowCount:String)
      |object $appname {
      | def main(args: Array[String]){
      |   val conf = new SparkConf()
      |     .setAppName("${appname}Cost")
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $encoders
      |   import spark.implicits._
      |   def genStat(n: String, s: Statistics): Stat = s.rowCount match {
      |     case Some(rc) => Stat(n, s.sizeInBytes.toString, rc.toString)
      |     case _ => Stat(n, s.sizeInBytes.toString, "-1")
      |   }
      |   $data
      |   $gcode
      | }
      |}""".stripMargin
  }

  def writeParagraph(appname: String, data: String, header: String, gcode: String, encoders: String): String  = {
    s"""
      |import org.apache.spark.sql._
      |import org.apache.spark.sql.functions._
      |import org.apache.spark.sql.catalyst.plans.logical._
      |import org.apache.spark.sql.types._
      |import org.apache.spark.sql.expressions.scalalang._
      |import sparkutils._
      |import sparkutils.loader._
      |import sparkutils.skew.SkewDataset._
      |import java.io._
      |$header
      |case class Stat(name: String, sizeInBytes:String, rowCount:String)
      |$encoders
      |import spark.implicits._
      |def genStat(n: String, s: Statistics): Stat = s.rowCount match {
      |  case Some(rc) => Stat(n, s.sizeInBytes.toString, rc.toString)
      |  case _ => Stat(n, s.sizeInBytes.toString, "-1")
      |}
      |$data
      |$gcode
    """.stripMargin

      // |val fname = "/Users/jac/code/trance/compiler/out"
      // |val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
  }



}