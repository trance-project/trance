package shredding.examples.genomic

import shredding.core._
import shredding.examples.Query

trait GenomicBase extends Query {
  
  def inputTypes(shred: Boolean = false): Map[Type, String] = 
    // todo handle shredded case, and organize genomic relations object
    GenomicRelations.q1inputs
  
  def headerTypes(shred: Boolean = false): List[String] = inputTypes(shred).values.toList
  
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
                    IfThenElse(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "altcnt" -> gref("call"))))))),
              List("contig", "start"),
              List("altcnt"),
              IntType)

  val program = Program(Assignment("Q", query))
}

object AlleleCounts extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val query = ForeachUnion(vdef, relV,
                //IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cohorts" ->
                    ForeachUnion(idef, relI, 
                      Singleton(Tuple("iscase" -> iref("iscase"), "altcnt" ->
                        Total(ForeachUnion(cdef, relC, 
                          IfThenElse(Cmp(OpEq, PrimitiveProject(iref, "iscase"), PrimitiveProject(cref, "iscase")),
                            ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              IfThenElse(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                                WeightedSingleton(Tuple("cnt" -> gref("call")), NumericProject(gref,"call")))))))))))))//)

  val program = Program(Assignment("Q", query))
}

object AlleleCounts2 extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val query = ForeachUnion(vdef, relV,
                //IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cohorts" ->
                    GroupBy(ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              ForeachUnion(cdef, relC, 
                                IfThenElse(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                                  Singleton(Tuple("pinfo" -> cref("iscase"), "cnt" -> gref("call")))))),
                      List("pinfo"),
                      List("cnt"),
                      IntType
                    ))))//)

  val program = Program(Assignment("Q", query))
}

object AlleleCounts3 extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val cdef2 = VarDef("c2", cdef.tp)
  val cref2 = TupleVarRef(cdef2)
  val query = ForeachUnion(vdef, relV,
                IfThenElse(Not(Cmp(OpEq, PrimitiveProject(vref, "consequence"), Const("LOW IMPACT", StringType))),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    ForeachUnion(idef, relI, 
                      Singleton(Tuple("case" -> iref("iscase"), "altcnt" -> 
                        Total(
                          ForeachUnion(cdef, relC,
                            IfThenElse(Cmp(OpEq, PrimitiveProject(iref, "iscase"), PrimitiveProject(cref, "iscase")),
                            ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              IfThenElse(And(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                                             Cmp(OpGt, PrimitiveProject(gref, "call"), Const(0, IntType))),
                                Singleton(Tuple("call" -> gref("call")))))))))))))))

  val program = Program(Assignment("Q", query))
}

object AlleleCountsGB extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val query = ForeachUnion(vdef, relV,
                Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                  GroupBy(ForeachUnion(gdef, BagProject(vref, "genotypes"),
                    ForeachUnion(cdef, relC,
                      IfThenElse(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                        Singleton(Tuple("case" -> cref("iscase"), "genotype" -> gref("call")))))),
                   List("case"),
                   List("genotype"),
                   IntType))))

  val program = Program(Assignment("Q", query))
}

object AlleleCountsGB3 extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val query = ForeachUnion(vdef, relV,
                IfThenElse(Cmp(OpEq, PrimitiveProject(vref, "consequence"), Const("LOW IMPACT", StringType)),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    GroupBy(ForeachUnion(gdef, BagProject(vref, "genotypes"),
                      ForeachUnion(cdef, relC,
                        IfThenElse(And(Cmp(OpEq, PrimitiveProject(gref, "sample"), PrimitiveProject(cref, "sample")),
                                      Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType))),
                        Singleton(Tuple("case" -> cref("iscase"), "altcnt" -> Const(1, IntType)))))),
                   List("case"),
                   List("altcnt"),
                   IntType)))))

  val program = Program(Assignment("Q", query))
}

object AlleleFG extends GenomicBase {
  val name = "AlleleFG"
  def inputs(tmap: Map[String, String]): String = ""
  val keys = DeDup(ForeachUnion(cdef, relC, Singleton(Tuple("pinfo" -> cref("iscase")))))
  val k = VarDef("k", keys.tp.asInstanceOf[BagType].tp)
  val kr = TupleVarRef(k)
  val query = Sequence(List(Named(VarDef("keys", keys.tp), keys),
                ForeachUnion(k, BagVarRef(VarDef("keys", keys.tp)),
                  Singleton(Tuple("case" -> kr("pinfo"), "variants" ->
                    ForeachUnion(vdef, relV, 
                      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "genotypes" ->
                        ForeachUnion(cdef, relC, 
                          IfThenElse(Cmp(OpEq, PrimitiveProject(cref, "iscase"), PrimitiveProject(kr, "pinfo")),
                            ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              IfThenElse(And(Cmp(OpEq, PrimitiveProject(cref, "sample"), PrimitiveProject(gref, "sample")),
                                           Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType))),
                                Singleton(Tuple("sample" -> cref("sample")))))))))))))))

  val program = Program(Assignment("Q", query))
}

object AlleleFG2 extends GenomicBase {
  val name = "AlleleFG2"
  def inputs(tmap: Map[String, String]): String = ""
  val keys = DeDup(ForeachUnion(cdef, relC, Singleton(Tuple("pinfo" -> cref("iscase")))))
  val k = VarDef("k", keys.tp.asInstanceOf[BagType].tp)
  val kr = TupleVarRef(k)
  val query = Sequence(List(Named(VarDef("keys", keys.tp), keys),
                ForeachUnion(vdef, relV, 
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "genotypes" ->
                  ForeachUnion(k, BagVarRef(VarDef("keys", keys.tp)),
                    Singleton(Tuple("case" -> kr("pinfo"), "samples" ->
                      ForeachUnion(cdef, relC, 
                        IfThenElse(Cmp(OpEq, PrimitiveProject(cref, "iscase"), PrimitiveProject(kr, "pinfo")),
                          ForeachUnion(gdef, BagProject(vref, "genotypes"),
                            IfThenElse(And(Cmp(OpEq, PrimitiveProject(cref, "sample"), PrimitiveProject(gref, "sample")),
                                           Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType))),
                              Singleton(Tuple("sample" -> cref("sample")))))))))))))))

  val program = Program(Assignment("Q", query))
}

object AlleleFG1 extends GenomicBase {
  val name = "AlleleFG1"
  def inputs(tmap: Map[String, String]): String = ""

  val query = ForeachUnion(vdef, relV, 
                Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "samples" ->
                  ForeachUnion(gdef, BagProject(vref, "genotypes"),
                    IfThenElse(Cmp(OpGt, NumericProject(gref, "call"), Const(0, IntType)),
                      ForeachUnion(cdef, relC,
                        IfThenElse(Cmp(OpEq, PrimitiveProject(cref, "sample"), PrimitiveProject(gref, "sample")),
                          Singleton(Tuple("pinfo" -> PrimitiveProject(cref, "iscase"), "sample" -> PrimitiveProject(gref, "sample"))))))))))

  val program = Program(Assignment("Q", query))
}
