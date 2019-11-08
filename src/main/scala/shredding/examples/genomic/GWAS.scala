package shredding.examples.genomic

import shredding.core._
import shredding.examples.Query
import shredding.nrc.LinearizedNRC
import shredding.wmcc._

trait GenomicBase extends Query {
  
  def inputTypes(shred: Boolean = false): Map[Type, String] = 
    // todo handle shredded case, and organize genomic relations object
    GenomicRelations.q1inputs
  
  def headerTypes(shred: Boolean = false): List[String] = inputTypes(shred).map(f => f._2).toList
  
  val relI = BagVarRef(VarDef("cases", BagType(GenomicRelations.casetype)))
  val idef = VarDef("i", GenomicRelations.casetype)
  val iref = TupleVarRef(idef)

  val relV = BagVarRef(VarDef("variants", BagType(GenomicRelations.varianttype)))
  val vdef = VarDef("v", GenomicRelations.varianttype)
  val vref = TupleVarRef(vdef)
  val gdef = VarDef("g", GenomicRelations.genotype)
  val gref = TupleVarRef(gdef)

  val relC = BagVarRef(VarDef("clinical", BagType(GenomicRelations.clintype)))
  val cdef = VarDef("c", GenomicRelations.clintype)
  val cref = TupleVarRef(cdef)

}

object AltCounts extends GenomicBase {

  val name = "AltCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  // for v in Variants union 
  //     (v.contig, v.start, 

  val query = GroupBy(ForeachUnion(vdef, relV,
                ForeachUnion(gdef, BagProject(vref, "genotypes"),
                  ForeachUnion(cdef, relC,
                    IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "altcnt" -> gref("call"))))))),
              List("contig", "start"),
              List("altcnt"),
              IntType)  
}

object AlleleCounts extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val query = ForeachUnion(vdef, relV,
                IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    DeDup(ForeachUnion(cdef, relC, 
                      Singleton(Tuple("case" -> cref("iscase"), "altcnt" -> 
                        Total(ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                                WeightedSingleton(Tuple("call" -> gref("call")), PrimitiveProject(gref, "call")))))))))))))
}

object AlleleCounts2 extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val query = ForeachUnion(vdef, relV,
                Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                  ForeachUnion(idef, relI, 
                    Singleton(Tuple("case" -> iref("iscase"), "altcnt" ->
                      Total(ForeachUnion(cdef, relC,
                              IfThenElse(Cmp(OpEq, iref("iscase"), cref("iscase")),
                                ForeachUnion(gdef, BagProject(vref, "genotypes"),
                                  IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                                    Singleton(Tuple("call" -> gref("call"))))))))))))))
}

object AlleleCounts3 extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val cdef2 = VarDef("c2", cdef.tp)
  val cref2 = TupleVarRef(cdef2)
  val query = ForeachUnion(vdef, relV,
                IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    ForeachUnion(idef, relI, 
                      Singleton(Tuple("case" -> iref("iscase"), "altcnt" -> 
                        Total(
                          ForeachUnion(cdef, relC,
                            IfThenElse(Cmp(OpEq, iref("iscase"), cref("iscase")),
                            ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              IfThenElse(And(Cmp(OpEq, gref("sample"), cref("sample")),
                                             Cmp(OpGt, gref("call"), Const(0, IntType))),
                                Singleton(Tuple("call" -> gref("call")))))))))))))))
}

object AlleleCountsGB extends GenomicBase {

  val name = "AlleleCounts"
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

object AlleleCountsGB3 extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val query = ForeachUnion(vdef, relV,
                IfThenElse(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType)),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    GroupBy(ForeachUnion(gdef, BagProject(vref, "genotypes"),
                      ForeachUnion(cdef, relC,
                        IfThenElse(And(Cmp(OpEq, gref("sample"), cref("sample")),
                                      Cmp(OpGt, gref("call"), Const(0, IntType))),
                        Singleton(Tuple("case" -> cref("iscase"), "altcnt" -> Const(1, IntType)))))),
                   List("case"),
                   List("altcnt"),
                   IntType)))))
}
