package framework.generator.spark

import java.io._
import framework.common._
import framework.plans._
import framework.examples.tpch._
import framework.examples.Query

/** 
  * Utility functions for generating Spark applications 
  */
object AppWriter {

  /** Standard pipeline: Dataset generator **/
  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"

  def flatDataset(query: Query, label: String, skew: Boolean = false, optLevel: Int = 2): Unit =
    runDataset(query, label, optLevel, skew)

  def writeLoader(name: String, tp: List[(String, Type)], header: Boolean = true, delimiter: String = ","): Unit = {
    val tmaps = Map(tp -> name)
    val fname = s"../executor/spark/src/main/scala/sparkutils/loader/${name}Loader.scala"
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val loaderc = SparkLoaderGenerator.generateLoader(name, tp, header, delimiter)
    printer.println(loaderc)
    printer.close
  }

  def runDataset(query: Query, label: String, optLevel: Int = 2, skew: Boolean = false): Unit = {
    
    val codegen = new SparkDatasetGenerator(false, false, optLevel = optLevel, skew = skew)
    val gcode = codegen.generate(query.anf(optimizationLevel = optLevel))
    val header = s"""|${codegen.generateHeader()}""".stripMargin
    val encoders = codegen.generateEncoders()

    val flatTag = optLevel match {
      case 0 => "None"
      case 1 => "Proj"
      case _ => ""
    }
    val qname = if (skew) s"${query.name}${flatTag}SkewSpark" else s"${query.name}${flatTag}Spark"
    val fname = s"$pathout/$qname.scala" 
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = query.loadTables(shred = false, skew = skew)
    val finalc = writeDataset(qname, inputs, header, timedOne(gcode), label, encoders)
    printer.println(finalc)
    printer.close 
  
  }

  def runDatasetInput(inputQuery: Query, query: Query, label: String, optLevel: Int = 2, skew: Boolean = false): Unit = {
    
    val codegenInput = new SparkDatasetGenerator(true, false, optLevel = optLevel, skew = skew)//,externalInputs = query.inputTypes(false))
    val inputCode = codegenInput.generate(inputQuery.anf()) 
    val codegen = new SparkDatasetGenerator(false, true, optLevel = optLevel, inputs = codegenInput.types, skew = skew) 
    val gcode = codegen.generate(query.anf(optimizationLevel = optLevel))
    val header = s"""|${codegen.generateHeader()}""".stripMargin
    val encoders = codegenInput.generateEncoders() + "\n" + codegen.generateEncoders()

    val flatTag = optLevel match {
      case 0 => "None"
      case 1 => "Proj"
      case _ => ""
    }
    val qname = if (skew) s"${query.name}${flatTag}SkewSpark" else s"${query.name}${flatTag}Spark"
    val fname = s"$pathout/$qname.scala" 
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = query.loadTables(shred = false, skew = skew)
    val finalc = writeDataset(qname, inputs, header, s"$inputCode\n${timedOne(gcode)}", label, encoders)
    printer.println(finalc)
    printer.close 
  }

  /** Shredded pipeline: Dataset generator **/

  def shredDataset(query: Query, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit =
      runDatasetShred(query, label, eliminateDomains, unshred, skew)

  def runDatasetShred(query: Query, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegen = new SparkDatasetGenerator(unshred, eliminateDomains, evalFinal = false, skew = skew)
    val (gcodeShred, gcodeUnshred) = query.shredBatchPlan(unshred, eliminateDomains = eliminateDomains, anfed = true)
    val gcode1 = codegen.generate(gcodeShred)
    val (header, gcodeSet, encoders) = if (unshred) {
      val codegen2 = new SparkDatasetGenerator(false, false, unshred = true, inputs = codegen.types, skew = skew)
      val ugcode = codegen2.generate(gcodeUnshred)
      val encoders1 = codegen.generateEncoders() +"\n"+ codegen2.generateEncoders()
      (s"""|${codegen2.generateHeader()}""".stripMargin, List(gcode1, ugcode), encoders1)
    } else 
      (s"""|${codegen.generateHeader()}""".stripMargin, List(gcode1), codegen.generateEncoders())
   
    val us = if (unshred) "Unshred" else ""
    val qname = if (skew) s"Shred${query.name}${us}SkewSpark" else s"Shred${query.name}${us}Spark"
    val fname = s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = query.loadTables(shred = true, skew = skew)
    val finalc = writeDataset(qname, inputs, header, timed(label, gcodeSet), label, encoders)
    printer.println(finalc)
    printer.close
  
  }

  def runDatasetInputShred(inputQuery: Query, query: Query, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegenInput = new SparkDatasetGenerator(true, true, evalFinal = false, skew = skew)
    val (inputShred, queryShred, queryUnshred) = query.shredBatchWithInput(inputQuery, unshredRun = unshred, eliminateDomains = eliminateDomains)
    val inputCode = codegenInput.generate(inputShred)
    val codegen = new SparkDatasetGenerator(unshred, eliminateDomains, evalFinal = !unshred, inputs = codegenInput.types, skew = skew)
    val gcode1 = codegen.generate(queryShred)
    val (header, gcodeSet, encoders) = if (unshred) {
      val codegen2 = new SparkDatasetGenerator(false, false, unshred = true, inputs = codegen.types, skew = skew)
      val ugcode = codegen2.generate(queryUnshred)
      val encoders1 = codegenInput.generateEncoders() +"\n"+ codegen.generateEncoders() +"\n"+ codegen2.generateEncoders()
      (s"""|${codegen2.generateHeader()}""".stripMargin, List(gcode1, ugcode), encoders1)
    } else 
      (s"""|${codegen.generateHeader()}""".stripMargin, List(gcode1), codegenInput.generateEncoders() +"\n"+ codegen.generateEncoders())
   
    val us = if (unshred) "Unshred" else ""
    val qname = if (skew) s"Shred${query.name}${us}SkewSpark" else s"Shred${query.name}${us}Spark"
    val fname = s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = query.loadTables(shred = true, skew = skew)
    val finalc = writeDataset(qname, inputs, header, s"$inputCode\n${timed(label, gcodeSet)}", label, encoders)
    printer.println(finalc)
    printer.close
  
  }

  /** Writes a generated application for a query using Spark Datasets **/
  def writeDataset(appname: String, data: String, header: String, gcode: String, label:String, encoders: String): String  = {
    s"""
      |package sparkutils.generated
      |/** Generated **/
      |import org.apache.spark.SparkConf
      |import org.apache.spark.sql.SparkSession
      |import org.apache.spark.sql._
      |import org.apache.spark.sql.functions._
      |import org.apache.spark.sql.types._
      |import org.apache.spark.sql.expressions.scalalang._
      |import sparkutils._
      |import sparkutils.loader._
      |import sparkutils.skew.SkewDataset._
      |$header
      |object $appname {
      | def main(args: Array[String]){
      |   val sf = Config.datapath.split("/").last
      |   val conf = new SparkConf().setMaster(Config.master)
      |     .setAppName(\"$appname\"+sf)
      |     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $encoders
      |   import spark.implicits._
      |   $data
      |   $gcode
      |   println("$label,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
      | }
      |}""".stripMargin
  }

  /**
    * Produces an ouptut spark application 
    * (either shredded or not shredded) that does unnesting
    */
  def timeOp(appname: String, e: String, i: Int = 0): String = {
    val query = if (i > 0) "unshredding" else "query"
    s"""
      |var start$i = System.currentTimeMillis()
      |$e
      |var end$i = System.currentTimeMillis() - start$i
      |println("$appname,"+sf+","+Config.datapath+","+end$i+",$query,"+spark.sparkContext.applicationId)
    """.stripMargin
  }

  def timed(appname: String, e: List[String], encoders: String = ""): String =
    s"""| def f = {
        | $encoders
        | ${e.zipWithIndex.map{ case (e1,i) => timeOp(appname, e1, i) }.mkString("\n")}
        |}
        |var start = System.currentTimeMillis()
        |f
        |var end = System.currentTimeMillis() - start
    """.stripMargin
   
  def timedOne(e: String, encoders: String = ""): String = {
    s"""|def f = { 
        | $encoders
        | $e
        |}
        |var start = System.currentTimeMillis()
        |f
        |var end = System.currentTimeMillis() - start """.stripMargin
  }


}
