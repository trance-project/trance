package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._

object Utils {

  val normalizer = new Finalizer(new BaseNormalizer{})
  val pathout = (outf: String) => s"src/test/scala/shredding/examples/tpch/$outf.Scala"
  //val pathout = (outf: String) => s"src/test/scala/shredding/examples/simple/$outf.Scala"

  /**
    * Produces an output file for a query pipeline
    * (either shredded or not shredded) the does not do unnesting
    */

  def runCalc(qInfo: (CExpr, String, String), inputM: Map[Type, String], 
              q2Info: (CExpr, String, String) = (EmptySng, "", "")): Unit = {
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val (q1, qname, qdata) = qInfo
    
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    val inputs = normq1 match {
                   case l @ LinearCSet(_) => inputM ++ l.getTypeMap
                   case _ => inputM ++ Map(normq1.tp.asInstanceOf[BagCType].tp -> s"${qname}Out")
                 }
    val ng = inputM.toList.map(f => f._2)
    val codegen = new ScalaNamedGenerator(inputs)
    
    val anfedq1 = new Finalizer(anfBase).finalize(normq1.asInstanceOf[CExpr])
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    val gcode = codegen.generate(anfExp1)
    val header = codegen.generateHeader(ng)

    val printer = new PrintWriter(new FileOutputStream(new File(pathout(qname+"Calc")), false))
    val finalc = write(qname+"Calc", qdata, header, gcode)
    printer.println(finalc)
    printer.close 

    // generate the down stream query
    if (q2Info != (EmptySng, "", "")){
      
      val (q2, q2name, q2data) = q2Info

      val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
      anfBase.reset
      val anfedq2 = anfer.finalize(normq2)
      val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])

      val gcode2 = codegen.generate(anfExp2)
      val header2 = codegen.generateHeader(ng)

      val printer2 = new PrintWriter(new FileOutputStream(new File(pathout(q2name+"Calc")), false))
      val finalc2 = write2(q2name+"Calc", qdata, header2, gcode, q2name, gcode2, q2data)
      printer2.println(finalc2)
      printer2.close 

    }

  }

  /**
    * Produces an output file for a query pipeline 
    * (either shredded or not shredded) that does unnesting
    */
 
  def run(qInfo: (CExpr, String, String), inputM: Map[Type, String], 
          q2Info: (CExpr, String, String) = (EmptySng, "", "")): Unit = {
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val (q1, qname, qdata) = qInfo
    
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    val inputs = normq1 match {
                  case l @ LinearCSet(_) => inputM ++ l.getTypeMap
                  case _ => inputM ++ Map(normq1.tp.asInstanceOf[BagCType].tp -> s"${qname}Out")
                 }
    val ng = inputM.toList.map(f => f._2)
    val codegen = new ScalaNamedGenerator(inputs)
    
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExp1))
    val gcode = codegen.generate(anfExp1)
    val header = codegen.generateHeader(ng)

    val printer = new PrintWriter(new FileOutputStream(new File(pathout(qname)), false))
    val finalc = write(qname, qdata, header, gcode)
    printer.println(finalc)
    printer.close 

    // generate the down stream query
    if (q2Info != (EmptySng, "", "")){
      val (q2, q2name, q2data) = q2Info

      val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
      println(Printer.quote(normq2))
      val plan2 = Unnester.unnest(normq2)(Nil, Nil, None)
      println(Printer.quote(plan2))
      anfBase.reset
      val anfedq2 = anfer.finalize(plan2)
      val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])

      val gcode2 = codegen.generate(anfExp2)
      val header2 = codegen.generateHeader(ng)

      val printer2 = new PrintWriter(new FileOutputStream(new File(pathout(q2name)), false))
      val finalc2 = write2(q2name, qdata, header2, gcode, q2name, gcode2, q2data)
      printer2.println(finalc2)
      printer2.close 
    }

  }

  /**
    * Produces an output file for a query pipeline
    * (either shredded or not shredded) the does not do unnesting
    * Currently, this does not handle Spark code that does nested
    * comprehensions with an input that is an RDD. The generated 
    * Spark application for those queries will error out during execution.
    */

  def runCalcSpark(qInfo: (CExpr, String, String), inputM: Map[Type, String], 
              q2Info: (CExpr, String, String) = (EmptySng, "", "")): Unit = {
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val (q1, qname, qdata) = qInfo
    
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1))
    val inputs = normq1 match {
                   case l @ LinearCSet(_) => inputM ++ l.getTypeMap
                   case _ => inputM ++ Map(normq1.tp.asInstanceOf[BagCType].tp -> s"${qname}Out")
                 }
    val ng = inputM.toList.map(f => f._2)
    val codegen = new SparkNamedGenerator(inputs)
    
    val anfedq1 = new Finalizer(anfBase).finalize(normq1.asInstanceOf[CExpr])
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    val gcode = codegen.generate(anfExp1)
    val header = codegen.generateHeader(ng)

    val printer = new PrintWriter(new FileOutputStream(new File(pathout(qname+"SparkCalc")), false))
    val finalc = writeSpark(qname+"SparkCalc", qdata, header, gcode)
    printer.println(finalc)
    printer.close 

    // generate the down stream query
    if (q2Info != (EmptySng, "", "")){
      
      val (q2, q2name, q2data) = q2Info

      val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
      anfBase.reset
      val anfedq2 = anfer.finalize(normq2)
      val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])

      val gcode2 = codegen.generate(anfExp2)
      val header2 = codegen.generateHeader(ng)

      val printer2 = new PrintWriter(new FileOutputStream(new File(pathout(q2name+"Calc")), false))
      val finalc2 = write2(q2name+"Calc", qdata, header2, gcode, q2name, gcode2, q2data)
      printer2.println(finalc2)
      printer2.close 

    }

  }

  /**
    * Produces an ouptut spark application 
    * (either shredded or not shredded) that does unnesting
    */
 
  def runSpark(qInfo: (CExpr, String, String), inputM: Map[Type, String], 
          q2Info: (CExpr, String, String) = (EmptySng, "", "")): Unit = {
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val (q1, qname, qdata) = qInfo
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(s"\n$qname")
    println(Printer.quote(normq1))
    val inputs = normq1 match {
                  case l @ LinearCSet(_) => inputM ++ l.getTypeMap
                  case _ => inputM ++ Map(normq1.tp.asInstanceOf[BagCType].tp -> s"${qname}Out")
                 }
    val ng = inputM.toList.map(f => f._2)
    val codegen = new SparkNamedGenerator(inputs)
    
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExp1))
    val gcode = codegen.generate(anfExp1)
    val header = codegen.generateHeader(ng)

    val printer = new PrintWriter(new FileOutputStream(new File(pathout(qname+"Spark")), false))
    val finalc = writeSpark(qname+"Spark", qdata, header, gcode)
    printer.println(finalc)
    printer.close 

    // generate the down stream query
    if (q2Info != (EmptySng, "", "")){
      val (q2, q2name, q2data) = q2Info

      val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
      println(Printer.quote(normq2))
      val plan2 = Unnester.unnest(normq2)(Nil, Nil, None)
      println(Printer.quote(plan2))
      anfBase.reset
      val anfedq2 = anfer.finalize(plan2)
      val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])

      val gcode2 = codegen.generate(anfExp2)
      val header2 = codegen.generateHeader(ng)

      val printer2 = new PrintWriter(new FileOutputStream(new File(pathout(q2name+"Spark")), false))
      val finalc2 = writeSpark2(q2name+"Spark", qdata, header2, gcode, q2name, gcode2, q2data)
      printer2.println(finalc2)
      printer2.close 
    }

  }

  /**
    * Writes out a query for a Spark application
    **/

  def writeSpark(appname: String, data: String, header: String, gcode: String): String  = {
    s"""
      |package experiments
      |/** Generated **/
      |import org.apache.spark.SparkConf
      |import org.apache.spark.sql.SparkSession
      |import sprkloader._
      |$header
      |object $appname {
      | def main(args: Array[String]){
      |   val conf = new SparkConf().setMaster(Config.master).setAppName(\"$appname\")
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $data
      |   var start0 = System.currentTimeMillis()
      |   $gcode.count
      |   var end0 = System.currentTimeMillis() - start0
      |   println($appname+","+Config.datapath+","+end0)
      | }
      |}""".stripMargin
  }

  /**
    * Writes out a query for a Spark application that takes a materialized query as input
    **/

  def writeSpark2(appname: String, data: String, header: String, gcode1: String, input: String, gcode: String, shred: String = ""): String  = {
    val inputquery = if (appname.startsWith("Shred")) { s"$gcode1 \n $shred" }
                     else { s"val $input = { $gcode1 } \n $input.cache \n $input.count" }
    s"""
      |package experiments
      |/** Generated **/
      |import org.apache.spark.SparkConf
      |import org.apache.spark.sql.SparkSession
      |import sprkloader._
      |$header
      |object $appname {
      | def main(args: Array[String]){
      |   val conf = new SparkConf().setMaster(Config.master).setAppName(\"$appname\")
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $data
      |   $inputquery
      |   var start0 = System.currentTimeMillis()
      |   def f() {
      |     $gcode.count
      |   }
      |   f
      |   var end0 = System.currentTimeMillis() - start0
      |   println($appname+","+Config.datapath+","+end0)
      | }
      |}""".stripMargin
  }


  /**
    * Writes out a query that has inputs provided from the context (h)
    */

  def write(n: String, i: String, h: String, q: String): String = {
    s"""
      |package experiments
      |/** Generated code **/
      |import shredding.core.CaseClassRecord
      |import shredding.examples.tpch._
      |    $h
      |object $n {
      | def main(args: Array[String]){
      |    var start0 = System.currentTimeMillis()
      |    var id = 0L
      |    def newId: Long = {
      |      val prevId = id
      |      id += 1
      |      prevId
      |    }
      |    $i
      |    var end0 = System.currentTimeMillis() - start0
      |    def f(){
      |      $q
      |    }
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {
      |      var start = System.currentTimeMillis()
      |      f
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    val avg = (time.sum/5)
      |    println(end0+","+avg)
      | }
      |}""".stripMargin
  }

  /**
    * Writes out a query that takes another query as input
    */
  
  def write2(n: String, i1: String, h: String, q1: String, i2: String, q2: String, ef: String = ""): String = {
    s"""
      |package experiments
      |/** Generated code **/
      |import shredding.core.CaseClassRecord
      |import shredding.examples.tpch._
      |$h
      |object $n {
      | def main(args: Array[String]){
      |    var start0 = System.currentTimeMillis()
      |    var id = 0L
      |    def newId: Long = {
      |      val prevId = id
      |      id += 1
      |      prevId
      |    }
      |    $i1
      |    val $i2 = { $q1 }
      |    var end0 = System.currentTimeMillis() - start0
      |    $ef
      |    def f(){
      |      $q2
      |    }
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {
      |     var start = System.currentTimeMillis()
      |      f
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    val avg = (time.sum/5)
      |    println(end0+","+avg)
      | }
      |}""".stripMargin
  }

}
