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

  val geneType = TupleType("name" -> StringType, "contig" -> StringType, "start" -> IntType, "end" -> IntType)
  val relGenes = BagVarRef(VarDef("genes", BagType(geneType)))
  val genedef = VarDef("ge", geneType)
  val generef = TupleVarRef(genedef)

}

// get burden by gene set
object GeneBurdenBySample3 extends GenomicBase{
  val name = "GeneBurdenBySample"
  def inputs(tmap: Map[String, String]): String = ""
  
  val query = 
  ForeachUnion(cdef, relC,
    Singleton(Tuple("sample" -> cref("sample"), "gene" -> ForeachUnion(genedef, relGenes,
      Singleton(Tuple("gene" -> generef("name"), "mutations" ->
        ForeachUnion(vdef, relV, 
          IfThenElse(And(Cmp(OpGt, generef("end"), vref("start")),
                     And(Cmp(OpGe, vref("start"), generef("start")), 
                     Cmp(OpEq, vref("contig"), generef("contig")))),
            ForeachUnion(gdef, BagProject(vref, "genotypes"),
              IfThenElse(Cmp(OpEq, cref("sample"), gref("sample")),
                  Singleton(gref)))))))))))
     
  
}


object GeneBurdenBySampleGB extends GenomicBase{
  val name = "GeneBurdenBySample"
  def inputs(tmap: Map[String, String]): String = ""
  
  val query = 
  ForeachUnion(cdef, relC,
    Singleton(Tuple("sample" -> cref("sample"), "case" -> cref("iscase"), "gene" -> 
    GroupBy(
    ForeachUnion(genedef, relGenes,
        ForeachUnion(vdef, relV, 
          IfThenElse(And(Cmp(OpGt, generef("end"), vref("start")),
                     And(Cmp(OpGe, vref("start"), generef("start")), 
                     Cmp(OpEq, vref("contig"), generef("contig")))),
            ForeachUnion(gdef, BagProject(vref, "genotypes"),
              IfThenElse(Cmp(OpEq, cref("sample"), gref("sample")),
                  Singleton(Tuple("gene" -> generef("name"), "call" ->gref("call")))))))),
    List("gene"),
    List("call"),
    IntType))))
     
  
}

object GeneByRegroup extends GenomicBase{
  val name = "GeneByRegroup"
  def inputs(tmap: Map[String, String]): String = ""
  
  val genegrps = VarDef("GeneGroups", GeneBurdenBySampleGB.query.tp)
  val g2 = VarDef("g2", genegrps.tp.asInstanceOf[BagType].tp)
  val gr2 = TupleVarRef(g2)

  val g3 = VarDef("g3", gr2("gene").tp.asInstanceOf[BagType].tp)
  val gr3 = TupleVarRef(g3)

  val query = ForeachUnion(g2, BagVarRef(genegrps), Singleton(gr2)) 
  /**  GroupBy(
      GroupBy(
        ForeachUnion(g2, BagVarRef(genegrps), 
          ForeachUnion(g3, BagProject(gr2, "gene"), 
            Singleton(Tuple("case" -> gr2("case"), "gene" -> gr3("gene"), "cnt" -> gr3("_2"))))),
              List("case", "gene"),
              List("cnt"),
              BagType(TupleType("fake" -> StringType))    
          ),
      List("case"),
      List("gene", "_2"),
      IntType
     )**/
  
}


/**
  * For every sample, calculate the gene burden for every gene.
  */
object GeneBurdenBySample extends GenomicBase{
  val name = "GeneBurdenBySample"
  def inputs(tmap: Map[String, String]): String = ""
  val query = ForeachUnion(cdef, relC, 
                Singleton(Tuple("sample" -> cref("sample"), "case" -> cref("iscase"), "genes" ->
                  ForeachUnion(genedef, relGenes,
                    Singleton(Tuple("name" -> generef("name"),
                      "burden" -> Total(ForeachUnion(vdef, relV,
                          IfThenElse(And(Cmp(OpGt, generef("end"), vref("start")),
                                      And(Cmp(OpGe, vref("start"), generef("start")), 
                                         Cmp(OpEq, vref("contig"), generef("contig")))),
                        ForeachUnion(gdef, BagProject(vref, "genotypes"),
                          IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                            Singleton(Tuple("call" -> gref("call"))))))))))))))
}

// this is a bug
object GeneBurdenBySample2 extends GenomicBase{
  val name = "GeneBurdenBySample"
  def inputs(tmap: Map[String, String]): String = ""

  val cbyv = ForeachUnion(cdef, relC, 
                Singleton(Tuple("sample" -> cref("sample"), "case" -> cref("iscase"), "genotypes" ->
                  ForeachUnion(vdef, relV, 
                    ForeachUnion(gdef, BagProject(vref, "genotypes"),
                      IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                        Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "call" -> gref("call"))))))
                  )))
  val samples = VarDef("samples", cbyv.tp)
  val sample = VarDef("sample", cbyv.tp.tp)
  val sampleRef = TupleVarRef(sample)

  val varis = VarDef("variant", TupleType("contig" -> StringType, "start" -> IntType, "call" -> IntType))
  val varir = TupleVarRef(varis)

  val query2 = ForeachUnion(sample, BagVarRef(samples),  
                Singleton(Tuple("sample" -> sampleRef("sample"), "case" -> cref("iscase"), "genes" ->
                    ForeachUnion(genedef, relGenes,
                      Singleton(Tuple("name" -> generef("name"),
                        "burden" -> ForeachUnion(varis, BagProject(sampleRef, "genotypes"),                                                      IfThenElse(And(Cmp(OpGt, generef("end"), vref("start")),
                                      And(Cmp(OpGe, vref("start"), generef("start")), 
                                         Cmp(OpEq, vref("contig"), generef("contig")))),
                                     WeightedSingleton(gref, PrimitiveProject(gref, "call"))))))))))
  val query = Sequence(List(Named(samples, cbyv), query2))
}


object GenotypesBySample extends GenomicBase {
  val name = "GenotypesBySample"
  def inputs(tmap: Map[String, String]): String = ""

  val query = ForeachUnion(cdef, relC, 
                Singleton(Tuple("sample" -> cref("sample"), "genotypes" ->
                  ForeachUnion(vdef, relV, 
                    ForeachUnion(gdef, BagProject(vref, "genotypes"),
                      IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                        Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "call" -> gref("call"))))))
                  )))
}

object GenotypesBySample2 extends GenomicBase {
  val name = "GenotypesBySample2"
  def inputs(tmap: Map[String, String]): String = ""

  val query = ForeachUnion(cdef, relC, 
                Singleton(Tuple("sample" -> cref("sample"), "genotypes" ->
                  ForeachUnion(vdef, relV, 
                    ForeachUnion(gdef, BagProject(vref, "genotypes"),
                      IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                        Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "call" -> gref("call"))))))
                  )))
}

object AltCounts extends GenomicBase {

  val name = "AltCounts"
  def inputs(tmap: Map[String, String]): String = ""
  
  val query = GroupBy(ForeachUnion(vdef, relV,
                ForeachUnion(gdef, BagProject(vref, "genotypes"),
                  ForeachUnion(cdef, relC,
                    IfThenElse(Cmp(OpEq, gref("sample"), cref("sample")),
                      Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "altcnt" -> gref("call"))))))),
              List("contig", "start"),
              List("altcnt"),
              IntType)  
}

object AlleleGroups extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val c2 = VarDef("c2", cdef.tp)
  val cr2 = TupleVarRef(c2)
  val query = ForeachUnion(vdef, relV,
                Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                  ForeachUnion(cdef, relC, 
                      Singleton(Tuple("case" -> cref("iscase"), "altcnt" -> 
                        ForeachUnion(c2, relC, 
                          IfThenElse(Cmp(OpEq, cref("iscase"), cr2("iscase")), 
                            ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              IfThenElse(And(Cmp(OpEq, gref("sample"), cr2("sample")), 
                                             Not(Cmp(OpEq, gref("call"), Const(0, IntType)))),
                                Singleton(Tuple("sample" -> cr2("iscase"), "call" -> gref("call")))))))))))))
}

object AlleleCounts extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val c2 = VarDef("c2", cdef.tp)
  val cr2 = TupleVarRef(c2)
  val query = ForeachUnion(vdef, relV,
                IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
                  Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    DeDup(ForeachUnion(cdef, relC, 
                      Singleton(Tuple("case" -> cref("iscase"), "altcnt" -> 
                        Total(
                          ForeachUnion(c2, relC, 
                            IfThenElse(Cmp(OpEq, cref("iscase"), cr2("iscase")), 
                          ForeachUnion(gdef, BagProject(vref, "genotypes"),
                              IfThenElse(Cmp(OpEq, gref("sample"), cr2("sample")),
                                WeightedSingleton(Tuple("call" -> gref("call")), PrimitiveProject(gref, "call")))))))))))))))
}

object AlleleCounts4 extends GenomicBase {

  val name = "AlleleCounts"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString(""   )}"
  
  val c2 = VarDef("c2", cdef.tp)
  val cr2 = TupleVarRef(c2)
  val query = 
    DeDup(ForeachUnion(cdef, relC,
      Singleton(Tuple("iscase" -> cref("iscase"), "mutations" ->
        ForeachUnion(vdef, relV, 
          IfThenElse(Not(Cmp(OpEq, vref("consequence"), Const("LOW IMPACT", StringType))),
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), "cases" ->
                    Total(ForeachUnion(c2, relC, 
                      IfThenElse(Cmp(OpEq, cref("iscase"), cr2("iscase")),
                        ForeachUnion(gdef, BagProject(vref, "genotypes"),
                          IfThenElse(Cmp(OpEq, gref("sample"), cr2("sample")),
                            WeightedSingleton(Tuple("call" -> gref("call")), PrimitiveProject(gref, "call")))))))))))))))
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

object AlleleCountsGB1 extends GenomicBase {

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
