package framework.examples.genomic

import framework.core._
import framework.nrc.MaterializeNRC

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
 
 val nrc = new MaterializeNRC{}
 import nrc._

 val relP = BagVarRef("pathways", BagType(PathwayRelations.pathtype))
 val pref = TupleVarRef("p", PathwayRelations.pathtype)
 
 val gref = TupleVarRef("g", PathwayRelations.genetype)
 
 val relA = BagVarRef("annotations", BagType(PathwayRelations.annottype))
 val aref = TupleVarRef("a", PathwayRelations.annottype)

 val sref = TupleVarRef("s", PathwayRelations.conseqtype)

  val relI = BagVarRef("cases", BagType(GenomicRelations.casetype))
  val iref = TupleVarRef("i", GenomicRelations.casetype)

  val relV = BagVarRef("variants", BagType(GenomicRelations.varianttype))
  val vref = TupleVarRef("v", GenomicRelations.varianttype)
  val g2ref = TupleVarRef("g2", GenomicRelations.genotype)

  val relC = BagVarRef("clinical", BagType(GenomicRelations.clintype))
  val cref = TupleVarRef("c", GenomicRelations.clintype)

 val q0 = ForeachUnion(pref, relP,
            Singleton(Tuple("pathway" -> pref("name"), "case_burden" -> 
              ForeachUnion(gref, pref("genes").asInstanceOf[BagExpr],
                ForeachUnion(vref, relV,
                  IfThenElse(And(Cmp(OpGe, vref("start"), gref("end")),
                             Cmp(OpGe, gref("start"), vref("start"))),
                    ForeachUnion(aref, relA,
                      IfThenElse(And(Cmp(OpEq, aref("contig"), vref("contig")),
                                 Cmp(OpEq, aref("start"), vref("start"))),
                        ForeachUnion(sref, aref("transcript_consequences").asInstanceOf[BagExpr],
                          IfThenElse(Not(Cmp(OpEq, sref("impact"), Const("LOW", StringType))),
                            ForeachUnion(iref, relI,
                              Singleton(Tuple("case" -> iref("iscase"), "mutation_burden" ->
                                ForeachUnion(cref, relC,
                                  IfThenElse(Cmp(OpEq, cref("iscase"), iref("iscase")),
                                    ForeachUnion(g2ref, vref("genotypes").asInstanceOf[BagExpr],
                                      IfThenElse(Cmp(OpEq, g2ref("sample"), cref("sample")),
                                        Singleton(Tuple("call" -> g2ref("call"))))))))))))))))))))
 // basic mutation burden
 // when you don't cast the bag exprs, compilation hangs
 val q1 = ForeachUnion(pref, relP,
            Singleton(Tuple("pathway" -> pref("name"), "case_burden" -> 
              ForeachUnion(gref, pref("genes").asInstanceOf[BagExpr],
                ForeachUnion(vref, relV,
                  IfThenElse(And(Cmp(OpGe, vref("start"), gref("end")),
                             Cmp(OpGe, gref("start"), vref("start"))),
                    ForeachUnion(aref, relA,
                      IfThenElse(And(Cmp(OpEq, aref("contig"), vref("contig")),
                                 Cmp(OpEq, aref("start"), vref("start"))),
                        ForeachUnion(sref, aref("transcript_consequences").asInstanceOf[BagExpr],
                          IfThenElse(Not(Cmp(OpEq, sref("impact"), Const("LOW", StringType))),
                            ForeachUnion(iref, relI,
                              Singleton(Tuple("case" -> iref("iscase"), "mutation_burden" ->
                                Count(ForeachUnion(cref, relC,
                                  IfThenElse(Cmp(OpEq, cref("iscase"), iref("iscase")),
                                    ForeachUnion(g2ref, vref("genotypes").asInstanceOf[BagExpr],
                                      IfThenElse(And(Cmp(OpEq, g2ref("sample"), cref("sample")),
                                                  Cmp(OpGt, g2ref("call"), NumericConst(0, IntType))),
                                        Singleton(Tuple("call" -> NumericConst(1, IntType)))))))))))))))))))))
                                       

  // annotated variants
  // requires genotype as an allele string
  // match with annotation, and find transcript annotations 
  // with non-low consequences
  // this will created a nested set of variant annotations 
  // that can be used for downstream queries
  
  val relV2 = BagVarRef("variants", BagType(GenomicRelations.varianttype))
  val v2ref = TupleVarRef("v", GenomicRelations.varianttype)

  val g3ref = TupleVarRef("g", GenomicRelations.genotype)
  
  /**val q2 = ForeachUnion(vdef, relV, 
            IfThenElse(
            ForeachUnion(g3def, vref("genotypes").asInstanceOf[BagExpr],
              IfThenElse(Cmp(OpGt, g3ref("call"), Const(0, IntType)),
                ForeachUnion(adef, relA, **/
 
} 
