package shredding.examples.genomic

import shredding.core._
import shredding.examples.Query

trait GenomicBase extends Query {
  
  def inputTypes(shred: Boolean = false): Map[Type, String] = 
    // todo handle shredded case, and organize genomic relations object
    GenomicRelations.q1inputs
  
  def headerTypes(shred: Boolean = false): List[String] = inputTypes(shred).values.toList
  
  val relI = BagVarRef("cases", BagType(GenomicRelations.casetype))
  val iref = TupleVarRef("i", GenomicRelations.casetype)

  val relV = BagVarRef("variants", BagType(GenomicRelations.varianttype))
  val vref = TupleVarRef("v", GenomicRelations.varianttype)
  val gref = TupleVarRef("g", GenomicRelations.genotype)

  val relC = BagVarRef("clinical", BagType(GenomicRelations.clintype))
  val cref = TupleVarRef("c", GenomicRelations.clintype)

}

object AltCounts extends GenomicBase {
  val name = "AltCounts"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val queryAC =
    ReduceByKey(
      ForeachUnion(vref, relV,
        ForeachUnion(gref, BagProject(vref, "genotypes"),
          ForeachUnion(cref, relC,
            IfThenElse(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
              Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "altcnt" -> gref("call"))))))),
      List("contig", "start"),
      List("altcnt"))

  val program = Program(Assignment(name, queryAC))
}

object AlleleCounts extends GenomicBase {
  val name = "AlleleCounts"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val queryAC =
    ForeachUnion(vref, relV,
      //IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cohorts" ->
        ForeachUnion(iref, relI,
          Singleton(Tuple("iscase" -> iref("iscase"), "altcnt" ->
            Sum(
              ForeachUnion(cref, relC,
                IfThenElse(Cmp(OpEq, PrimitiveProject(iref, "iscase"), PrimitiveProject(cref, "iscase")),
                  ForeachUnion(gref, BagProject(vref, "genotypes"),
                    IfThenElse(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                      Singleton(Tuple("cnt" -> gref("call"))))))),
              List("cnt")
            )("cnt")
          ))))))//)

  val program = Program(Assignment(name, queryAC))
}

object AlleleCounts2 extends GenomicBase {
  val name = "AlleleCounts2"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val queryAC2 =
    ForeachUnion(vref, relV,
      //IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cohorts" ->
        ReduceByKey(
          ForeachUnion(gref, BagProject(vref, "genotypes"),
            ForeachUnion(cref, relC,
              IfThenElse(
                Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                Singleton(Tuple("pinfo" -> cref("iscase"), "cnt" -> gref("call")))))),
          List("pinfo"),
          List("cnt")
        ))))//)

  val program = Program(Assignment(name, queryAC2))
}

object AlleleCounts3 extends GenomicBase {
  val name = "AlleleCounts3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val queryAC3 = ForeachUnion(vref, relV,
                IfThenElse(Not(Cmp(OpEq, PrimitiveProject(vref, "consequence"), Const("LOW IMPACT", StringType))),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    ForeachUnion(iref, relI,
                      Singleton(Tuple("case" -> iref("iscase"), "altcnt" -> 
                        Count(
                          ForeachUnion(cref, relC,
                            IfThenElse(Cmp(OpEq, PrimitiveProject(iref, "iscase"), PrimitiveProject(cref, "iscase")),
                            ForeachUnion(gref, BagProject(vref, "genotypes"),
                              IfThenElse(And(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                                             Cmp(OpGt, PrimitiveProject(gref, "call"), Const(0, IntType))),
                                Singleton(Tuple("call" -> gref("call")))))))))))))))

  val program = Program(Assignment(name, queryAC3))
}

object AlleleCountsGB extends GenomicBase {
  val name = "AlleleCountsGB"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val queryACGB =
    ForeachUnion(vref, relV,
      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
        ReduceByKey(
          ForeachUnion(gref, BagProject(vref, "genotypes"),
            ForeachUnion(cref, relC,
              IfThenElse(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                Singleton(Tuple("case" -> cref("iscase"), "genotype" -> gref("call")))))),
          List("case"),
          List("genotype")
        ))))

  val program = Program(Assignment(name, queryACGB))
}

object AlleleCountsGB3 extends GenomicBase {
  val name = "AlleleCountsGB3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val queryACGB3 =
    ForeachUnion(vref, relV,
      IfThenElse(Cmp(OpEq, PrimitiveProject(vref, "consequence"), Const("LOW IMPACT", StringType)),
        Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
          ReduceByKey(
            ForeachUnion(gref, BagProject(vref, "genotypes"),
              ForeachUnion(cref, relC,
                IfThenElse(And(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                  Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType))),
                  Singleton(Tuple("case" -> cref("iscase"), "altcnt" -> Const(1, IntType)))))),
            List("case"),
            List("altcnt")
          )))))

  val program = Program(Assignment(name, queryACGB3))
}

object AlleleFG extends GenomicBase {
  val name = "AlleleFG"

  def inputs(tmap: Map[String, String]): String = ""

  val keys = DeDup(ForeachUnion(cref, relC, Singleton(Tuple("pinfo" -> cref("iscase")))))
  val kr = TupleVarRef("k", keys.tp.asInstanceOf[BagType].tp)

  val queryAFG = ForeachUnion(kr, BagVarRef("keys", keys.tp),
                Singleton(Tuple("case" -> kr("pinfo"), "variants" ->
                  ForeachUnion(vref, relV,
                    Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "genotypes" ->
                      ForeachUnion(cref, relC,
                        IfThenElse(Cmp(OpEq, PrimitiveProject(cref, "iscase"), PrimitiveProject(kr, "pinfo")),
                          ForeachUnion(gref, BagProject(vref, "genotypes"),
                            IfThenElse(And(Cmp(OpEq, PrimitiveProject(cref, "sample"), PrimitiveProject(gref, "sample")),
                                         Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType))),
                              Singleton(Tuple("sample" -> cref("sample")))))))))))))

  val program = Program(Assignment("keys", keys), Assignment(name, queryAFG))
}

object AlleleFG2 extends GenomicBase {
  val name = "AlleleFG2"

  def inputs(tmap: Map[String, String]): String = ""

  val keys = DeDup(ForeachUnion(cref, relC, Singleton(Tuple("pinfo" -> cref("iscase")))))
  val kr = TupleVarRef("k", keys.tp.asInstanceOf[BagType].tp)

  val queryAFG2 = ForeachUnion(vref, relV,
                Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "genotypes" ->
                ForeachUnion(kr, BagVarRef("keys", keys.tp),
                  Singleton(Tuple("case" -> kr("pinfo"), "samples" ->
                    ForeachUnion(cref, relC,
                      IfThenElse(Cmp(OpEq, PrimitiveProject(cref, "iscase"), PrimitiveProject(kr, "pinfo")),
                        ForeachUnion(gref, BagProject(vref, "genotypes"),
                          IfThenElse(And(Cmp(OpEq, PrimitiveProject(cref, "sample"), PrimitiveProject(gref, "sample")),
                                         Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType))),
                            Singleton(Tuple("sample" -> cref("sample")))))))))))))

  val program = Program(Assignment("keys", keys), Assignment(name, queryAFG2))
}

object AlleleFG1 extends GenomicBase {
  val name = "AlleleFG1"

  def inputs(tmap: Map[String, String]): String = ""

  val queryAFG1 = ForeachUnion(vref, relV,
                Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "samples" ->
                  ForeachUnion(gref, BagProject(vref, "genotypes"),
                    IfThenElse(Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType)),
                      ForeachUnion(cref, relC,
                        IfThenElse(Cmp(OpEq, PrimitiveProject(cref, "sample"), PrimitiveProject(gref, "sample")),
                          Singleton(Tuple("pinfo" -> PrimitiveProject(cref, "iscase"), "sample" -> PrimitiveProject(gref, "sample"))))))))))

  val program = Program(Assignment(name, queryAFG1))
}
