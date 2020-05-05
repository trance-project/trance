package framework.generator.spark

import java.io._
import framework.core._
import framework.plans._
import framework.examples.tpch._
import framework.examples.Query

/** 
  * Utility functions for generating Spark applications 
  */
object AppWriter {

  /** Standard pipeline: Dataset generator **/

  def flatDataset(query: Query, path: String, label: String, skew: Boolean = false, opt: Int = 2): Unit =
    runDataset(query, path, label, opt, skew)

  def runDataset(query: Query, pathout: String, label: String, 
    optLevel: Int = 2, skew: Boolean = false): Unit = {
    
    val codegen = new SparkDatasetGenerator(false, false, isDict = false, skew = skew)//,inputs = query.inputTypes(false))
    val gcode = codegen.generate(query.anf(optLevel))
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
    val inputs = if (skew) query.inputs(TPCHSchema.skewdfs) else query.inputs(TPCHSchema.dfs)
    val finalc = writeDataset(qname, inputs, header, timedOne(gcode), label, encoders)
    printer.println(finalc)
    printer.close 
  
  }

  def runDatasetInput(inputQuery: Query, query: Query, pathout: String, label: String, 
    optLevel: Int = 2, skew: Boolean = false): Unit = {
    
    val codegenInput = new SparkDatasetGenerator(true, false, isDict = false, skew = skew)//,externalInputs = query.inputTypes(false))
    val inputCode = codegenInput.generate(inputQuery.anf()) 
    val codegen = new SparkDatasetGenerator(false, true, isDict = false, inputs = codegenInput.types, skew = skew) 
    val gcode = codegen.generate(query.anf(optLevel))
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
    val inputs = if (skew) query.inputs(TPCHSchema.skewdfs) else query.inputs(TPCHSchema.dfs)
    val finalc = writeDataset(qname, inputs, header, s"$inputCode\n${timedOne(gcode)}", label, encoders)
    printer.println(finalc)
    printer.close 
  }

  /** Shredded pipeline: Dataset generator **/

  def shredDataset(query: Query, path: String, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit =
      runDatasetShred(query, path, label, eliminateDomains, unshred, skew)

  def runDatasetShred(query: Query, pathout: String, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegen = new SparkDatasetGenerator(unshred, eliminateDomains, skew = skew)
    val (gcodeShred, gcodeUnshred) = query.shredPlan(unshred, eliminateDomains = eliminateDomains, anfed = true)
    val gcode1 = codegen.generate(gcodeShred)
    val (header, gcodeSet, encoders) = if (unshred) {
      val codegen2 = new SparkDatasetGenerator(false, false, unshred = true, inputs = codegen.types, skew = skew)
      val ugcode = codegen2.generate(gcodeUnshred)
      val encoders1 = codegen.generateEncoders() +"\n"+ codegen2.generateEncoders()
      (s"""|${codegen2.generateHeader(query.headerTypes(false))}""".stripMargin, List(gcode1, ugcode), encoders1)
    } else 
      (s"""|${codegen.generateHeader(query.headerTypes(true))}""".stripMargin, List(gcode1), codegen.generateEncoders())
   
    val us = if (unshred) "Unshred" else ""
    val qname = if (skew) s"Shred${query.name}${us}SkewSpark" else s"Shred${query.name}${us}Spark"
    val fname = s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = if (skew) query.inputs(TPCHSchema.sskewdfs) else query.inputs(TPCHSchema.sdfs)
    val finalc = writeDataset(qname, inputs, header, timed(label, gcodeSet), label, encoders)
    printer.println(finalc)
    printer.close
  
  }

  def runDatasetInputShred(inputQuery: Query, query: Query, pathout: String, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegenInput = new SparkDatasetGenerator(false, false, isDict = true, evalFinal = false, skew = skew)
    val (inputShred, queryShred, queryUnshred) = query.shredWithInput(inputQuery, unshredRun = unshred, eliminateDomains = eliminateDomains)
    val inputCode = codegenInput.generate(inputShred)
    val codegen = new SparkDatasetGenerator(unshred, eliminateDomains, isDict = true, inputs = codegenInput.types, skew = skew)
    val gcode1 = codegen.generate(queryShred)
    val (header, gcodeSet, encoders) = if (unshred) {
      val codegen2 = new SparkDatasetGenerator(false, false, unshred = true, isDict = true, inputs = codegen.types, skew = skew)
      val ugcode = codegen2.generate(queryUnshred)
      val encoders1 = codegenInput.generateEncoders() +"\n"+ codegen.generateEncoders() +"\n"+ codegen2.generateEncoders()
      (s"""|${codegen2.generateHeader(query.headerTypes(false))}""".stripMargin, List(gcode1, ugcode), encoders1)
    } else 
      (s"""|${codegen.generateHeader(query.headerTypes(true))}""".stripMargin, List(gcode1), codegenInput.generateEncoders() +"\n"+ codegen.generateEncoders())
   
    val us = if (unshred) "Unshred" else ""
    val qname = if (skew) s"Shred${query.name}${us}SkewSpark" else s"Shred${query.name}${us}Spark"
    val fname = s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputSection = s"${inputCode}\n${shredInputs(inputQuery.indexedDict)}"
    val inputs = if (skew) query.inputs(TPCHSchema.sskewdfs) else query.inputs(TPCHSchema.sdfs)
    val finalc = writeDataset(qname, inputs, header, s"$inputSection\n${timed(label, gcodeSet)}", label, encoders)
    printer.println(finalc)
    printer.close
  
  }

  /** Stanard pipeline: RDD generator **/

  def flat(query: Query, path: String, label: String): Unit =
    runSpark(query, path, label, 0, false)

  def flatInput(input: Query, query: Query, path: String, label: String): Unit =
    runSparkInput(input, query, path, label, 0, false)

  def flatProj(query: Query, path: String, label: String): Unit =
    runSpark(query, path, label, 1, false)

  def flatProjInput(input: Query, query: Query, path: String, label: String): Unit =
    runSparkInput(input, query, path, label, 1, false)

  def flatOpt(query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSpark(query, path, label, 2, skew)

  def flatOptInput(input: Query, query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSparkInput(input, query, path, label, 2, skew)

  def runSpark(query: Query, pathout: String, label: String, 
    optLevel: Int = 2, skew: Boolean = false): Unit = {
    
    val codegen = new SparkNamedGenerator(false, true, flatDict = true)
    val gcode = codegen.generate(query.anf(optLevel))
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            |import sprkloader.SkewTopRDD._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(false))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(false))}""".stripMargin
      }
   
    val flatTag = optLevel match {
      case 0 => "None"
      case 1 => "Proj"
      case _ => ""
    }
    val qname = if (skew) s"${query.name}${flatTag}SkewSpark" else s"${query.name}${flatTag}Spark"
    val fname = s"$pathout/$qname.scala" 
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = if (skew) query.inputs(TPCHSchema.skewcmds) else query.inputs(TPCHSchema.tblcmds)
    val finalc = writeSpark(qname, inputs, header, timedOne(gcode), label)
    printer.println(finalc)
    printer.close 
  
  }

  def runSparkInput(inputQuery: Query, query: Query, pathout: String, label: String, 
    optLevel: Int = 2, skew: Boolean = false): Unit = {
    
    val codegenInput = new SparkNamedGenerator(true, true, flatDict = true)
    val inputCode = codegenInput.generate(inputQuery.anf()) 
    val codegen = new SparkNamedGenerator(false, true, flatDict = true, inputs = codegenInput.types) 
    val gcode = codegen.generate(query.anf(optLevel))
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            |import sprkloader.SkewTopRDD._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(false))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(false))}""".stripMargin
      }
    val flatTag = optLevel match {
      case 0 => "None"
      case 1 => "Proj"
      case _ => ""
    }
    val qname = if (skew) s"${query.name}${flatTag}SkewSpark" else s"${query.name}${flatTag}Spark"
    val fname = s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = if (skew) query.inputs(TPCHSchema.skewcmds) else query.inputs(TPCHSchema.tblcmds)
    val finalc = writeSpark(qname, inputs, header, s"$inputCode\n${timedOne(gcode)}", label)
    printer.println(finalc)
    printer.close 
  
  }

  /** Shredded pipeline - RDD generator **/

  def shred(query: Query, path: String, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit =
      runSparkShred(query, path, label, eliminateDomains, unshred, skew)

  def shredInput(input: Query, query: Query, path: String, label: String, 
    eliminateDomains: Boolean = true, unshred: Boolean = false, skew: Boolean = false): Unit =
    runSparkInputShred(input, query, path, label, eliminateDomains, unshred, skew)

  def runSparkShred(query: Query, pathout: String, label: String, eliminateDomains: Boolean = true, 
    unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegen = new SparkNamedGenerator(unshred, eliminateDomains, flatDict = true)
    val (gcodeShred, gcodeUnshred) = query.shredPlan(unshred, eliminateDomains = eliminateDomains)
    val gcode1 = codegen.generate(gcodeShred)
    val gcodeSet = if (unshred) List(gcode1, codegen.generate(gcodeUnshred)) else List(gcode1)
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            |import sprkloader.SkewDictRDD._
            |import sprkloader.SkewTopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            |import sprkloader.DictRDDOperations._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      }
   
    val qname = if (skew) s"Shred${query.name}SkewSpark" else s"Shred${query.name}Spark"
    val fname = if (unshred) s"$pathout/unshred/$qname.scala" else s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = if (skew) query.inputs(TPCHSchema.sskewcmds) else query.inputs(TPCHSchema.stblcmds)
    val finalc = writeSpark(qname, inputs, header, timed(label, gcodeSet), label)
    printer.println(finalc)
    printer.close 
  
  }

  def runSparkInputShred(inputQuery: Query, query: Query, pathout: String, label: String, 
    eliminateDomains: Boolean = true, unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegenInput = new SparkNamedGenerator(false, false, flatDict = true)
    val (inputShred, queryShred, queryUnshred) = query.shredWithInput(inputQuery, unshredRun = unshred, eliminateDomains = eliminateDomains)
    val inputCode = codegenInput.generate(inputShred)
    val codegen = new SparkNamedGenerator(unshred, eliminateDomains, flatDict = true, inputs = codegenInput.types)
    val gcode1 = codegen.generate(queryShred)
    val gcodeSet = if (unshred) List(gcode1, codegen.generate(queryUnshred)) else List(gcode1)
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            |import sprkloader.SkewDictRDD._
            |import sprkloader.SkewTopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            |import sprkloader.DictRDDOperations._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      }
   
    val domains = if (eliminateDomains) "" else "Domains"
    val qname = if (skew) s"Shred${query.name}${domains}SkewSpark" else s"Shred${query.name}${domains}Spark"
    val fname = if (unshred) s"$pathout/unshred/$qname.scala" else s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputSection = s"${inputCode}\n${shredInputs(inputQuery.indexedDict)}"
    val inputs = if (skew) query.inputs(TPCHSchema.sskewcmds) else query.inputs(TPCHSchema.stblcmds)
    val finalc = writeSpark(qname, inputs, header, s"$inputSection\n${timed(label, gcodeSet)}", label)
    printer.println(finalc)
    printer.close 
  
  }

  def inputs(n: String, e: String): String = {
    s"""|val $n = {
        | $e
        |}
        |$n.cache
        |$n.evaluate""".stripMargin
  }
 
  def shredInputs(ns: List[String]): String = { 
    var cnt = 0
    ns.map{ n => 
      val (inputTag, outputTag) = if (cnt == 0) ("MBag", "IBag") else ("MDict", "IDict")
      val iname = n.replace("__D", "")
      val oname = n.replace("_1", "")
      cnt += 1
      s"""|val ${outputTag}_$oname = ${inputTag}_$iname
          |${outputTag}_$oname.cache
          |${outputTag}_$oname.count"""
    }.mkString("\n").stripMargin
  }

  /**
    * Writes out a query for a Spark application
    **/

  def writeSpark(appname: String, data: String, header: String, gcode: String, label:String): String  = {
    s"""
      |package experiments
      |/** Generated **/
      |import org.apache.spark.SparkConf
      |import org.apache.spark.sql.SparkSession
      |import scala.collection.mutable.HashMap
      |import sprkloader._
      |$header
      |object $appname {
      | def main(args: Array[String]){
      |   val sf = Config.datapath.split("/").last
      |   val conf = new SparkConf().setMaster(Config.master).setAppName(\"$appname\"+sf)
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $data
      |   $gcode
      |   println("$label,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
      | }
      |}""".stripMargin
  }

  def writeDataset(appname: String, data: String, header: String, gcode: String, label:String, encoders: String): String  = {
    s"""
      |package sprkloader.experiments
      |/** Generated **/
      |import org.apache.spark.SparkConf
      |import org.apache.spark.sql.SparkSession
      |import org.apache.spark.sql._
      |import org.apache.spark.sql.functions._
      |import org.apache.spark.sql.types._
      |import org.apache.spark.sql.expressions.scalalang._
      |import sprkloader._
      |import sprkloader.SkewDataset._
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
    val query = if (i > 0) "unframework" else "query"
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
