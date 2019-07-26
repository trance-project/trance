package shredding.examples.genomic

import shredding.core._
import shredding.nrc.LinearizedNRC

case class Clinical(sample: String, iscase: String)
case class Genotype(sample: String, call: Int)
case class Case(iscase: String)
case class Variant(contig: String, start: Int, genotypes: List[Genotype])

object GenomicRelations{

   val casetype = TupleType("iscase" -> StringType)
   val clintype = TupleType("sample" -> StringType, "iscase" -> StringType)
   val genotype = TupleType("sample" -> StringType, "call" -> IntType)
   val varianttype = TupleType("contig" -> StringType, "start" -> IntType, "genotypes" -> BagType(genotype))
   val q1inputs = Map[Type, String](casetype -> "Case", clintype -> "Clinical", 
                    genotype -> "Genotype", varianttype -> "Variant")

   val format1Spark = s"""
    |val cases = spark.sparkContext.parallelize(List(Case("control"), Case("case")))
    |val variants = spark.sparkContext.parallelize(List(
    |   Variant("1", 100, List(Genotype("one", 0), Genotype("two", 1), Genotype("three", 2), Genotype("four", 0))),
    |   Variant("1", 101, List(Genotype("one", 1), Genotype("two", 1), Genotype("three", 0), Genotype("four", 1))),
    |   Variant("1", 102, List(Genotype("one", 2), Genotype("two", 0), Genotype("three", 1), Genotype("four", 2)))))
    |val clinical = spark.sparkContext.parallelize(List(
    |    Clinical("one", "case"), Clinical("two", "case"), Clinical("three", "control"), Clinical("four", "control")))
    """.stripMargin 

}

object GenomicTests {

  val nrc = new LinearizedNRC{}
  import nrc._

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

  val q1 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), 
              "counts" -> ForeachUnion(idef, relI, 
                Singleton(Tuple("iscase" -> iref("iscase"), "altcnt" -> 
                  Total(ForeachUnion(cdef, relC, 
                    IfThenElse(Cmp(OpEq, cref("iscase"), iref("iscase")),
                      ForeachUnion(gdef, BagProject(vref, "genotypes"),
                        IfThenElse(Cmp(OpEq, cref("sample"), gref("sample")), 
                          IfThenElse(Cmp(OpEq, gref("call"), Const(1, IntType)), 
                            Singleton(Tuple("count" -> Const("alt", StringType))), 
                            IfThenElse(Cmp(OpEq, gref("call"), Const(2, IntType)), 
                              WeightedSingleton(Tuple("count" -> Const("alt", StringType)), Const(2, IntType)))))))))))))))  

  // count the number of genotypes
  val q2 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  ForeachUnion(cdef, relC, 
                                    IfThenElse(Cmp(OpEq, cref("sample"), gref("sample")),
                                      Singleton(Tuple("alt" -> Const("alt", StringType))))))))))
  
  // count the number of genotypes for case, but not control 
  val q3 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  ForeachUnion(cdef, relC, 
                                    IfThenElse(
                                      And(Cmp(OpEq, cref("sample"), gref("sample")),
                                          Cmp(OpEq, cref("iscase"), Const("case", StringType))),
                                      Singleton(Tuple("alt" -> Const("alt", StringType))))))))))

  // count the number of mutations that have at least one alternate allele for case, but not control 
  val q4 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  IfThenElse(Cmp(OpGt, gref("call"), Const(0, IntType)), 
                                    ForeachUnion(cdef, relC, 
                                      IfThenElse(
                                        And(Cmp(OpEq, cref("sample"), gref("sample")),
                                          Cmp(OpEq, cref("iscase"), Const("case", StringType))),
                                        Singleton(Tuple("alt" -> Const("alt", StringType)))))))))))

  val q5 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  IfThenElse(Cmp(OpGt, gref("call"), Const(0, IntType)), 
                                    ForeachUnion(cdef, relC, 
                                      IfThenElse(
                                        And(Cmp(OpEq, cref("sample"), gref("sample")),
                                          Cmp(OpEq, cref("iscase"), Const("case", StringType))),
                                        IfThenElse(
                                          Cmp(OpEq, gref("call"), Const(1, IntType)),
                                          Singleton(Tuple("alt" -> Const("alt", StringType))),
                                          WeightedSingleton(Tuple("alt" -> Const("alt", StringType)), Const(2, IntType)))))))))))


}
