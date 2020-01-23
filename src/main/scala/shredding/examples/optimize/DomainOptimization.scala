package shredding.examples.optimize

import shredding.core._
import shredding.examples.tpch.TPCHBase
import shredding.examples.genomic.GenomicBase

/**
  All label extractions are at top level
**/
object DomainOptExample1 extends TPCHBase {
  val name = "DomainOptExample1"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query = 
  ForeachUnion(c, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO,
      IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
        Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(l, relL,
          IfThenElse(
            Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
            ForeachUnion(p, relP, IfThenElse(
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))
}

object DomainOptExample2 extends TPCHBase {
  val name = "DomainOptExample2"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query = 
  ForeachUnion(c, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO,
      IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
        Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(l, relL,
            ForeachUnion(p, relP, IfThenElse(
              And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))))))))))
}

object DomainOptExample3 extends TPCHBase {
  val name = "DomainOptExample3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(l, relL,
        ForeachUnion(p, relP, IfThenElse(
          And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
            Singleton(Tuple("p_name" -> pr("p_name"), "customers" ->
              ForeachUnion(c, relC, IfThenElse(
                Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                  Singleton(Tuple("c_name" -> cr("c_name")))))))))))))
}

object DomainOptExample4 extends TPCHBase {
  val name = "DomainOptExample4"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(p, relP, 
        ForeachUnion(l, relL, IfThenElse(
          And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
            Singleton(Tuple("p_name" -> pr("p_name"), "customers" ->
              ForeachUnion(c, relC, IfThenElse(
                Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                  Singleton(Tuple("c_name" -> cr("c_name")))))))))))))

}

object DomainOptExample5 extends TPCHBase {
  val name = "DomainOptExample5"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val query =
    ForeachUnion(o, relO,
      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "custparts" ->
        ForeachUnion(c, relC, 
          IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
            ForeachUnion(p, relP, 
              ForeachUnion(l, relL, 
                IfThenElse(And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                            Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
                  Singleton(Tuple("c_name" -> cr("c_name"), "p_name" -> pr("p_name")))))))))))
}

object DomainOptExample6 extends GenomicBase {
 
  val name = "DomainOptExample6"
 
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"

  val query = ForeachUnion(vdef, relV,
                Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                  GroupBy(ForeachUnion(gdef, BagProject(vref, "genotypes"),
                    ForeachUnion(cdef, relC,
                      IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                        Singleton(Tuple("case" -> cref("iscase"), "genotype" -> gref("call")))))),
                   List("case"),
                   List("genotype"),
                   IntType))))
}
