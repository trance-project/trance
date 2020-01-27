package shredding.examples.genomic

import shredding.core._
import shredding.nrc.ShredNRC

case class Gene(name: String, contig: String, start: Int, end: Int)
case class Pathway(name: String, description: String, genes: List[Gene])
case class Consequence(impact: String)
case class Annotation(contig: String, start: Int, end: Int, most_severe_consequence: String, allele: String, transcript_consequence: List[Consequence])

object PathwayRelations{
 
 val genetype = TupleType("name" -> StringType, "contig" -> StringType, "start" -> IntType, "end" -> IntType)
 val pathtype = TupleType("name" -> StringType, "description" -> StringType, "genes" -> BagType(genetype))
 val conseqtype = TupleType("impact" -> StringType)
 val annottype = TupleType("contig" -> StringType, "start" -> IntType, "end" -> IntType, "most_severe_consequence" -> StringType, "allele" -> StringType, "transcript_consequences" -> BagType(conseqtype))
  
}

object PathwayTests {
 
 val nrc = new ShredNRC{}
 import nrc._

 val relP = BagVarRef(VarDef("pathways", BagType(PathwayRelations.pathtype)))
 val pdef = VarDef("p", PathwayRelations.pathtype)
 val pref = TupleVarRef(pdef)
 
 val gdef = VarDef("g", PathwayRelations.genetype)
 val gref = TupleVarRef(gdef)
 
 val relA = BagVarRef(VarDef("annotations", BagType(PathwayRelations.annottype)))
 val adef = VarDef("a", PathwayRelations.annottype)
 val aref = TupleVarRef(adef)

 val sdef = VarDef("s", PathwayRelations.conseqtype)
 val sref = TupleVarRef(sdef)

  val relI = BagVarRef(VarDef("cases", BagType(GenomicRelations.casetype)))
  val idef = VarDef("i", GenomicRelations.casetype)
  val iref = TupleVarRef(idef)

  val relV = BagVarRef(VarDef("variants", BagType(GenomicRelations.varianttype)))
  val vdef = VarDef("v", GenomicRelations.varianttype)
  val vref = TupleVarRef(vdef)
  val g2def = VarDef("g2", GenomicRelations.genotype)
  val g2ref = TupleVarRef(g2def)

  val relC = BagVarRef(VarDef("clinical", BagType(GenomicRelations.clintype)))
  val cdef = VarDef("c", GenomicRelations.clintype)
  val cref = TupleVarRef(cdef)

 val q0 = ForeachUnion(pdef, relP, 
            Singleton(Tuple("pathway" -> pref("name"), "case_burden" -> 
              ForeachUnion(gdef, pref("genes").asInstanceOf[BagExpr],
                ForeachUnion(vdef, relV, 
                  IfThenElse(And(Cmp(OpGe, vref("start"), gref("end")),
                             Cmp(OpGe, gref("start"), vref("start"))),
                    ForeachUnion(adef, relA, 
                      IfThenElse(And(Cmp(OpEq, aref("contig"), vref("contig")),
                                 Cmp(OpEq, aref("start"), vref("start"))),
                        ForeachUnion(sdef, aref("transcript_consequences").asInstanceOf[BagExpr], 
                          IfThenElse(Not(Cmp(OpEq, sref("impact"), Const("LOW", StringType))),
                            ForeachUnion(idef, relI, 
                              Singleton(Tuple("case" -> iref("iscase"), "mutation_burden" ->
                                ForeachUnion(cdef, relC, 
                                  IfThenElse(Cmp(OpEq, cref("iscase"), iref("iscase")),
                                    ForeachUnion(g2def, vref("genotypes").asInstanceOf[BagExpr],
                                      IfThenElse(Cmp(OpEq, g2ref("sample"), cref("sample")),
                                        Singleton(Tuple("call" -> g2ref("call"))))))))))))))))))))
 // basic mutation burden
 // when you don't cast the bag exprs, compilation hangs
 val q1 = ForeachUnion(pdef, relP, 
            Singleton(Tuple("pathway" -> pref("name"), "case_burden" -> 
              ForeachUnion(gdef, pref("genes").asInstanceOf[BagExpr],
                ForeachUnion(vdef, relV, 
                  IfThenElse(And(Cmp(OpGe, vref("start"), gref("end")),
                             Cmp(OpGe, gref("start"), vref("start"))),
                    ForeachUnion(adef, relA, 
                      IfThenElse(And(Cmp(OpEq, aref("contig"), vref("contig")),
                                 Cmp(OpEq, aref("start"), vref("start"))),
                        ForeachUnion(sdef, aref("transcript_consequences").asInstanceOf[BagExpr], 
                          IfThenElse(Not(Cmp(OpEq, sref("impact"), Const("LOW", StringType))),
                            ForeachUnion(idef, relI, 
                              Singleton(Tuple("case" -> iref("iscase"), "mutation_burden" ->
                                Total(ForeachUnion(cdef, relC, 
                                  IfThenElse(Cmp(OpEq, cref("iscase"), iref("iscase")),
                                    ForeachUnion(g2def, vref("genotypes").asInstanceOf[BagExpr],
                                      IfThenElse(And(Cmp(OpEq, g2ref("sample"), cref("sample")),
                                                  Cmp(OpGt, g2ref("call"), NumericConst(0, IntType))),
                                        Singleton(Tuple("call" -> NumericConst(1, IntType)))))))))))))))))))))
                                       

  // annotated variants
  // requires genotype as an allele string
  // match with annotation, and find transcript annotations 
  // with non-low consequences
  // this will created a nested set of variant annotations 
  // that can be used for downstream queries
  
  val relV2 = BagVarRef(VarDef("variants", BagType(GenomicRelations.varianttype)))
  val v2def = VarDef("v", GenomicRelations.varianttype)
  val v2ref = TupleVarRef(vdef)

  val g3def = VarDef("g", GenomicRelations.genotype)
  val g3ref = TupleVarRef(g3def)
  
  /**val q2 = ForeachUnion(vdef, relV, 
            IfThenElse(
            ForeachUnion(g3def, vref("genotypes").asInstanceOf[BagExpr],
              IfThenElse(Cmp(OpGt, g3ref("call"), Const(0, IntType)),
                ForeachUnion(adef, relA, **/
 
} 
