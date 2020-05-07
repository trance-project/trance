package framework.examples.optimize

import framework.common._
import framework.examples.tpch.TPCHBase
import framework.examples.genomic.GenomicBase

/**
  All label extractions are at top level
**/
object DomainOptExample1 extends TPCHBase {
  val name = "DomainOptExample1"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query1 =
  ForeachUnion(cr, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
      IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
        Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(lr, relL,
          IfThenElse(
            Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
            ForeachUnion(pr, relP, IfThenElse(
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))

  val program = Program(Assignment(name, query1))
}

object DomainOptExample2 extends TPCHBase {
  val name = "DomainOptExample2"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query2 =
  ForeachUnion(cr, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
      IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
        Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(lr, relL,
            ForeachUnion(pr, relP, IfThenElse(
              And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))))))))))

  val program = Program(Assignment(name, query2))
}

object DomainOptExample3 extends TPCHBase {
  val name = "DomainOptExample3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query3 =
    ForeachUnion(or, relO,
      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(lr, relL,
        ForeachUnion(pr, relP, IfThenElse(
          And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
            Singleton(Tuple("p_name" -> pr("p_name"), "customers" ->
              ForeachUnion(cr, relC, IfThenElse(
                Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                  Singleton(Tuple("c_name" -> cr("c_name")))))))))))))

  val program = Program(Assignment(name, query3))
}

object DomainOptExample4 extends TPCHBase {
  val name = "DomainOptExample4"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query4 =
    ForeachUnion(or, relO,
      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(pr, relP,
        ForeachUnion(lr, relL, IfThenElse(
          And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
            Singleton(Tuple("p_name" -> pr("p_name"), "customers" ->
              ForeachUnion(cr, relC, IfThenElse(
                Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                  Singleton(Tuple("c_name" -> cr("c_name")))))))))))))

  val program = Program(Assignment(name, query4))
}

object DomainOptExample5 extends TPCHBase {
  val name = "DomainOptExample5"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query5 =
    ForeachUnion(or, relO,
      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "custparts" ->
        ForeachUnion(cr, relC,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
            ForeachUnion(pr, relP,
              ForeachUnion(lr, relL,
                IfThenElse(And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                            Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
                  Singleton(Tuple("c_name" -> cr("c_name"), "p_name" -> pr("p_name")))))))))))

  val program = Program(Assignment(name, query5))
}

object DomainOptExample6 extends GenomicBase {
  val name = "DomainOptExample6"
 
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"

  val query6 =
    ForeachUnion(vref, relV,
      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
        ReduceByKey(
          ForeachUnion(gref, BagProject(vref, "genotypes"),
            ForeachUnion(cref, relC,
              IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                Singleton(Tuple("case" -> cref("iscase"), "genotype" -> gref("call")))))),
          List("case"),
          List("genotype")
        ))))

  val program = Program(Assignment(name, query6))
}
